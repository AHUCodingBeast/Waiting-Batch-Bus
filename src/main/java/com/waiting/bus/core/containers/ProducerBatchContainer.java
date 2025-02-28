package com.waiting.bus.core.containers;

import com.google.common.util.concurrent.ListenableFuture;
import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.constant.MessageProcessResultEnum;
import com.waiting.bus.core.ext.Callback;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.core.models.Result;
import com.waiting.bus.core.support.task.SendProducerBatchTask;
import com.waiting.bus.core.support.utils.DataSizeCalculator;
import com.waiting.bus.core.support.utils.IOThreadPool;
import com.waiting.bus.exceptions.ProducerException;
import com.waiting.bus.exceptions.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author jianzhang
 * @date 2024/1/31
 */
public class ProducerBatchContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerBatchContainer.class);

    private volatile boolean closed;

    private final Semaphore semaphore;

    private final AtomicInteger appendsInProgress;

    private final ProducerConfig producerConfig;

    private final Function<List<Message>, MessageProcessResultEnum> messageProcessFunction;

    private final BlockingQueue<ProducerBatch> successQueue;

    private final BlockingQueue<ProducerBatch> failureQueue;

    private final RetryQueue retryQueue;

    private final IOThreadPool ioThreadPool;

    // 当前内存总计有多少攒批
    private final AtomicInteger batchCount;

    private final ConcurrentMap<String, ProducerBatchHolder> batches;


    public ProducerBatchContainer(ProducerConfig producerConfig,
                                  Function<List<Message>, MessageProcessResultEnum> messageProcessFunction,
                                  Semaphore semaphore,
                                  RetryQueue retryQueue,
                                  BlockingQueue<ProducerBatch> successQueue,
                                  BlockingQueue<ProducerBatch> failureQueue,
                                  IOThreadPool ioThreadPool,
                                  AtomicInteger batchCount) {
        this.producerConfig = producerConfig;
        this.messageProcessFunction = messageProcessFunction;
        this.semaphore = semaphore;
        this.retryQueue = retryQueue;
        this.successQueue = successQueue;
        this.failureQueue = failureQueue;
        this.ioThreadPool = ioThreadPool;
        this.batchCount = batchCount;
        this.batches = new ConcurrentHashMap<>();
        this.appendsInProgress = new AtomicInteger(0);
        this.closed = false;
    }


    public ListenableFuture<Result> append(List<Message> items, Callback callback, String groupName) throws InterruptedException, ProducerException {
        appendsInProgress.incrementAndGet();
        try {
            return doAppend(items, callback, groupName);
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    public void append(List<Message> items, String groupName) throws InterruptedException, ProducerException {
        appendsInProgress.incrementAndGet();
        try {
            doAppend(items, groupName);
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    public List<ProducerBatch> getRemainingBatches() {
        if (!closed) {
            throw new IllegalStateException("cannot get the remaining batches before the log accumulator closed");
        }
        List<ProducerBatch> remainingBatches = new ArrayList<>();
        while (appendsInProgress.get() > 0) {
            extractTo(remainingBatches);
        }
        extractTo(remainingBatches);
        batches.clear();
        return remainingBatches;
    }

    public List<ProducerBatch> getExpiredBatches() {
        long nowMs = System.currentTimeMillis();
        List<ProducerBatch> expireBatches = new ArrayList<>();
        for (Map.Entry<String, ProducerBatchHolder> entry : batches.entrySet()) {
            ProducerBatchHolder holder = entry.getValue();
            synchronized (holder) {
                if (holder.producerBatch == null) {
                    continue;
                }
                long curRemainingMs = holder.producerBatch.remainingMs(nowMs, producerConfig.getLingerMs());
//                LOGGER.warn("groupName={} remainingMs={} createTime={}", holder.producerBatch.getGroupName(), curRemainingMs, holder.producerBatch.getCreatedMs());
                if (curRemainingMs <= 0) {
                    expireBatches.add(holder.producerBatch);
                    holder.producerBatch = null;
                }
            }
        }
        return expireBatches;
    }


    private ListenableFuture<Result> doAppend(List<Message> messageList, Callback callback, String groupName) throws InterruptedException, ProducerException {
        if (closed) {
            throw new IllegalStateException("cannot append after the waitingBus container was closed");
        }

        int sizeInBytes = DataSizeCalculator.calculateMessageByteSize(messageList);
        ensureSize(sizeInBytes);

        long maxBlockMs = producerConfig.getMaxBlockMs();
        if (maxBlockMs >= 0) {
            boolean acquired = semaphore.tryAcquire(sizeInBytes, maxBlockMs, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new TimeoutException("failed to acquire memory within the configured max blocking time " + producerConfig.getMaxBlockMs() + " ms");
            }
        } else {
            semaphore.acquire(sizeInBytes);
        }

        try {
            ProducerBatchHolder holder = getOrCreateProducerBatchHolder(groupName);
            synchronized (holder) {
                return appendToHolder(groupName, messageList, callback, sizeInBytes, holder);
            }
        } catch (Exception e) {
            semaphore.release(sizeInBytes);
            throw new ProducerException(e);
        }
    }

    private void doAppend(List<Message> messageList, String groupName) throws InterruptedException, ProducerException {
        if (closed) {
            throw new IllegalStateException("cannot append after the waitingBus container was closed");
        }

        int sizeInBytes = DataSizeCalculator.calculateMessageByteSize(messageList);
        ensureSize(sizeInBytes);

        long maxBlockMs = producerConfig.getMaxBlockMs();
        if (maxBlockMs >= 0) {
            boolean acquired = semaphore.tryAcquire(sizeInBytes, maxBlockMs, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new TimeoutException("failed to acquire memory within the configured max blocking time " + producerConfig.getMaxBlockMs() + " ms");
            }
        } else {
            semaphore.acquire(sizeInBytes);
        }

        try {
            ProducerBatchHolder holder = getOrCreateProducerBatchHolder(groupName);
            synchronized (holder) {
                appendToHolder(groupName, messageList, sizeInBytes, holder);
            }
        } catch (Exception e) {
            semaphore.release(sizeInBytes);
            throw new ProducerException(e);
        }
    }


    private ProducerBatchHolder getOrCreateProducerBatchHolder(String groupName) {
        ProducerBatchHolder holder = batches.get(groupName);
        if (holder != null) {
            return holder;
        }
        holder = new ProducerBatchHolder();
        ProducerBatchHolder previous = batches.putIfAbsent(groupName, holder);
        if (previous == null) {
            return holder;
        } else {
            return previous;
        }
    }

    private synchronized ListenableFuture<Result> appendToHolder(String groupName, List<Message> messages, Callback callback, int sizeInBytes, ProducerBatchHolder holder) {
        if (holder.producerBatch != null) {
            ListenableFuture<Result> f = holder.producerBatch.tryAppend(messages, sizeInBytes, callback);
            if (f != null) {
                if (holder.producerBatch.isMeetSendCondition()) {
                    holder.transferProducerBatch(ioThreadPool, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue);
                }
                return f;
            } else {
                holder.transferProducerBatch(ioThreadPool, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue);
            }
        }
        holder.producerBatch = new ProducerBatch(producerConfig.getBatchSizeThresholdInBytes(), producerConfig.getBatchCountThreshold(), producerConfig.getMaxReservedAttempts(), groupName);
        LOGGER.info("groupName={} has created a new batch  batchId={}", groupName, holder.producerBatch.getBatchId());
        ListenableFuture<Result> f = holder.producerBatch.tryAppend(messages, sizeInBytes, callback);
        batchCount.incrementAndGet();
        if (holder.producerBatch.isMeetSendCondition()) {
            holder.transferProducerBatch(ioThreadPool, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue);
        }
        return f;
    }


    private synchronized void appendToHolder(String groupName, List<Message> messages, int sizeInBytes, ProducerBatchHolder holder) {
        if (holder.producerBatch != null) {
            boolean appendResult = holder.producerBatch.tryAppend(messages, sizeInBytes);
            if (appendResult) {
                if (holder.producerBatch.isMeetSendCondition()) {
                    holder.transferProducerBatch(ioThreadPool, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue);
                }
                return;
            } else {
                holder.transferProducerBatch(ioThreadPool, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue);
            }
        }
        holder.producerBatch = new ProducerBatch(producerConfig.getBatchSizeThresholdInBytes(), producerConfig.getBatchCountThreshold(), producerConfig.getMaxReservedAttempts(), groupName);
        LOGGER.info("groupName={} has created a new batch  batchId={}", groupName, holder.producerBatch.getBatchId());
        holder.producerBatch.tryAppend(messages, sizeInBytes);
        batchCount.incrementAndGet();
        if (holder.producerBatch.isMeetSendCondition()) {
            holder.transferProducerBatch(ioThreadPool, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue);
        }
    }


    private void ensureSize(int sizeInBytes) {
        if (sizeInBytes > ProducerConfig.MAX_BATCH_SIZE_IN_BYTES) {
            throw new RuntimeException("the messageList is " + sizeInBytes + " bytes which is larger than MAX_BATCH_SIZE_IN_BYTES " +
                    ProducerConfig.MAX_BATCH_SIZE_IN_BYTES);
        }
        if (sizeInBytes > producerConfig.getTotalSizeInBytes()) {
            throw new RuntimeException("the messageList is " + sizeInBytes + " bytes which is larger than the totalSizeInBytes you specified");
        }
    }


    private void extractTo(List<ProducerBatch> producerBatches) {
        for (Map.Entry<String, ProducerBatchHolder> entry : batches.entrySet()) {
            ProducerBatchHolder holder = entry.getValue();
            synchronized (holder) {
                if (holder.producerBatch == null) {
                    continue;
                }
                producerBatches.add(holder.producerBatch);
                holder.producerBatch = null;
            }
        }
    }


    public void close() {
        this.closed = true;
    }

    private static final class ProducerBatchHolder {

        ProducerBatch producerBatch;

        void transferProducerBatch(IOThreadPool ioThreadPool, ProducerConfig producerConfig, Function<List<Message>, MessageProcessResultEnum> sendFunction,
                                   RetryQueue retryQueue, BlockingQueue<ProducerBatch> successQueue, BlockingQueue<ProducerBatch> failureQueue) {
            if (producerBatch == null) {
                return;
            }
            ioThreadPool.submit(new SendProducerBatchTask(producerBatch, producerConfig, sendFunction, retryQueue, successQueue, failureQueue));
            producerBatch = null;
        }
    }
}

