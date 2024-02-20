package com.waiting.bus.core;

import com.google.common.util.concurrent.ListenableFuture;
import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.core.containers.ProducerBatch;
import com.waiting.bus.core.containers.ProducerBatchContainer;
import com.waiting.bus.core.containers.RetryQueue;
import com.waiting.bus.core.ext.Callback;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.constant.MessageProcessResultEnum;
import com.waiting.bus.core.models.Result;
import com.waiting.bus.core.support.handler.BatchHandler;
import com.waiting.bus.core.support.handler.ExpireBatchHandler;
import com.waiting.bus.core.support.utils.IOThreadPool;
import com.waiting.bus.exceptions.ProducerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author jianzhang
 * @date 2024/2/5
 */
public class MessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProducer.class);

    /**
     * 攒批容器管理类
     */
    private final ProducerBatchContainer producerBatchContainer;

    /**
     * 内存控制
     */
    private final Semaphore memoryCtrl;

    /**
     * 批次计数器
     */
    private final AtomicInteger batchCount = new AtomicInteger(0);


    private final RetryQueue retryQueue;
    private final BatchHandler successBatchHandler;
    private final BatchHandler failureBatchHandler;
    private final ExpireBatchHandler expireBatchHandler;

    private final IOThreadPool ioThreadPool;

    private static final String SUCCESS_BATCH_HANDLER_SUFFIX = "-success-batch-handler";
    private static final String FAILURE_BATCH_HANDLER_SUFFIX = "-failure-batch-handler";
    private static final String EXPIRE_BATCH_HANDLER_SUFFIX = "-expire-batch-handler";

    private static final String COMPONENT_NAME = "WaitingBus";

    public static final String DEFAULT_GROUP_NAME = "WAITING-BUS-DEFAULT-GROUP";


    /**
     * 核心构造器
     *
     * @param producerConfig         生产者配置也就是攒批配置
     * @param messageProcessFunction 消息处理函数，由攒批控制器来在合适的时机进行调用
     */
    public MessageProducer(ProducerConfig producerConfig, Function<List<Message>, MessageProcessResultEnum> messageProcessFunction) {

        BlockingQueue<ProducerBatch> successQueue = new LinkedBlockingQueue<>();
        BlockingQueue<ProducerBatch> failureQueue = new LinkedBlockingQueue<>();
        retryQueue = new RetryQueue();

        ioThreadPool = new IOThreadPool(producerConfig.getIoThreadCount(), COMPONENT_NAME);
        this.memoryCtrl = new Semaphore(producerConfig.getTotalSizeInBytes());

        this.producerBatchContainer = new ProducerBatchContainer(producerConfig, messageProcessFunction, memoryCtrl, retryQueue, successQueue, failureQueue, ioThreadPool, batchCount);

        successBatchHandler = new BatchHandler(COMPONENT_NAME + SUCCESS_BATCH_HANDLER_SUFFIX, successQueue, this.batchCount, this.memoryCtrl);
        failureBatchHandler = new BatchHandler(COMPONENT_NAME + FAILURE_BATCH_HANDLER_SUFFIX, failureQueue, this.batchCount, this.memoryCtrl);
        expireBatchHandler = new ExpireBatchHandler(COMPONENT_NAME + EXPIRE_BATCH_HANDLER_SUFFIX, producerConfig, messageProcessFunction, producerBatchContainer, retryQueue,
                successQueue, failureQueue, ioThreadPool);


        successBatchHandler.start();
        failureBatchHandler.start();
        expireBatchHandler.start();

    }


    /**
     * 向攒批队列中追加消息
     *
     * @param groupName   攒批队列分组名
     * @param messageList 消息列表
     * @param callback    回调
     * @return ListenableFuture
     * @throws InterruptedException
     * @throws ProducerException
     */
    public ListenableFuture<Result> send(String groupName, List<Message> messageList, Callback callback) throws InterruptedException, ProducerException {

        messageList.forEach(message -> {
            message.setGroupName(groupName);
        });

        return producerBatchContainer.append(messageList, callback, groupName);
    }

    /**
     * 向攒批队列中追加消息,无回调设计
     *
     * @param groupName   攒批队列分组名
     * @param messageList 消息列表
     * @throws InterruptedException
     * @throws ProducerException
     */
    public void send(String groupName, List<Message> messageList) throws InterruptedException, ProducerException {
        if (groupName == null || groupName.isEmpty()) {
            groupName = DEFAULT_GROUP_NAME;
        }
        for (Message message : messageList) {
            message.setGroupName(groupName);
        }
        producerBatchContainer.append(messageList, groupName);
    }


    public void close() throws InterruptedException, ProducerException {
        close((long) Integer.MAX_VALUE);
    }

    public void close(Long timeoutMs) throws InterruptedException, ProducerException {
        long remainTimeoutMs = timeoutMs;
        remainTimeoutMs = closeExpireBatchHandler(remainTimeoutMs);
        remainTimeoutMs = closeIOThreadPool(remainTimeoutMs);
        remainTimeoutMs = closeSuccessBatchHandler(remainTimeoutMs);
        remainTimeoutMs = closeFailureBatchHandler(remainTimeoutMs);

        LOGGER.warn("message producer has been gracefully closed , congratulations!, remainTimeoutMs={}", remainTimeoutMs);

    }

    public int getBatchCount() {
        return batchCount.get();
    }

    public int availableMemoryInBytes() {
        return memoryCtrl.availablePermits();
    }


    private long closeExpireBatchHandler(long timeoutMs) throws InterruptedException, ProducerException {
        long startMs = System.currentTimeMillis();
        LOGGER.info("Closing the ExpireBatchHandler, timeoutMs={}", timeoutMs);

        producerBatchContainer.close();
        retryQueue.close();

        expireBatchHandler.close();
        expireBatchHandler.join(timeoutMs);
        if (expireBatchHandler.isAlive()) {
            throw new ProducerException("the ExpireBatchHandler is still alive");
        }
        long nowMs = System.currentTimeMillis();
        LOGGER.info("Close the ExpireBatchHandler success! , cost={}", nowMs - startMs);
        return timeoutMs - (nowMs - startMs);
    }

    private long closeSuccessBatchHandler(long timeoutMs) throws InterruptedException, ProducerException {
        long startMs = System.currentTimeMillis();
        successBatchHandler.close();
        boolean invokedFromCallback = Thread.currentThread() == this.successBatchHandler;
        if (invokedFromCallback) {
            // 避免自己关闭自己
            return timeoutMs;
        }
        successBatchHandler.join(timeoutMs);
        if (successBatchHandler.isAlive()) {
            throw new ProducerException("the success batch handler thread is still alive");
        }
        long nowMs = System.currentTimeMillis();
        LOGGER.info("Close the SuccessBatchHandler success! , cost={}", nowMs - startMs);
        return Math.max(0, timeoutMs - nowMs + startMs);
    }

    private long closeFailureBatchHandler(long timeoutMs)
            throws InterruptedException, ProducerException {
        long startMs = System.currentTimeMillis();
        failureBatchHandler.close();
        boolean invokedFromCallback =
                Thread.currentThread() == this.successBatchHandler
                        || Thread.currentThread() == this.failureBatchHandler;
        if (invokedFromCallback) {
            // 避免自己关闭自己
            return timeoutMs;
        }
        failureBatchHandler.join(timeoutMs);
        if (failureBatchHandler.isAlive()) {
            throw new ProducerException("the failure batch handler thread is still alive");
        }
        long nowMs = System.currentTimeMillis();
        LOGGER.info("Close the FailureBatchHandler success! , cost={}", nowMs - startMs);
        return Math.max(0, timeoutMs - nowMs + startMs);
    }


    private long closeIOThreadPool(long timeoutMs) throws InterruptedException, ProducerException {
        long startMs = System.currentTimeMillis();
        ioThreadPool.shutdown();
        if (ioThreadPool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
            LOGGER.debug("The ioThreadPool is terminated");
        } else {
            LOGGER.warn("The ioThreadPool is not fully terminated");
            throw new ProducerException("the ioThreadPool is not fully terminated");
        }
        long nowMs = System.currentTimeMillis();
        return Math.max(0, timeoutMs - nowMs + startMs);
    }


}
