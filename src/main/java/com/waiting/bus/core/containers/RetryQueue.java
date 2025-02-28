package com.waiting.bus.core.containers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryQueue.class);

    private final DelayQueue<ProducerBatch> retryDelayBatchesQueue = new DelayQueue<ProducerBatch>();

    private final AtomicInteger putsInProgress;

    private volatile boolean closed;

    public RetryQueue() {
        this.putsInProgress = new AtomicInteger(0);
        this.closed = false;
    }

    public void put(ProducerBatch batch) {
        putsInProgress.incrementAndGet();
        try {
            if (closed) {
                throw new IllegalStateException("cannot put after the retry queue was closed");
            }
            retryDelayBatchesQueue.put(batch);
        } finally {
            putsInProgress.decrementAndGet();
        }
    }

    public List<ProducerBatch> getExpiredBatches(long checkTime) {

        // 最多等到这个时间 就返回
        long deadline = System.currentTimeMillis() + checkTime;
        List<ProducerBatch> expiredBatches = new ArrayList<ProducerBatch>();
        // 获取已经超时的批次
        retryDelayBatchesQueue.drainTo(expiredBatches);
        if (!expiredBatches.isEmpty()) {
            return expiredBatches;
        }
        while (true) {
            if (checkTime < 0) {
                break;
            }
            ProducerBatch batch;
            try {
                batch = retryDelayBatchesQueue.poll(checkTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.info("Interrupted when poll batch from the retry batches");
                break;
            }
            if (batch == null) {
                break;
            }
            retryDelayBatchesQueue.drainTo(expiredBatches);
            expiredBatches.add(batch);
            if (!expiredBatches.isEmpty()) {
                break;
            }
            checkTime = deadline - System.currentTimeMillis();
        }
        return expiredBatches;
    }

    public List<ProducerBatch> getRemainingBatches() {
        if (!closed) {
            throw new IllegalStateException(
                    "cannot get the remaining batches before the retry queue closed");
        }
        while (true) {
            if (!putsInProgress()) {
                break;
            }
        }
        List<ProducerBatch> remainingBatches = new ArrayList<ProducerBatch>(retryDelayBatchesQueue);
        retryDelayBatchesQueue.clear();
        return remainingBatches;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
    }

    private boolean putsInProgress() {
        return putsInProgress.get() > 0;
    }
}
