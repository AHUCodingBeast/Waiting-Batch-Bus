package com.waiting.bus.core.support.handler;


import com.waiting.bus.core.containers.ProducerBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jianzhang
 * @date 2024/2/5
 */
public class BatchHandler extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);
    private final BlockingQueue<ProducerBatch> batches;

    private final AtomicInteger batchCount;

    private final Semaphore memoryController;

    private volatile boolean closed;


    public BatchHandler(String name, BlockingQueue<ProducerBatch> batches, AtomicInteger batchCount, Semaphore memoryController) {
        setDaemon(true);
        setUncaughtExceptionHandler(
                (t, e) -> LOGGER.error("Uncaught error in thread, name={}, e=", name, e));
        this.batches = batches;
        this.batchCount = batchCount;
        this.memoryController = memoryController;
        this.closed = false;
    }


    @Override
    public void run() {
        loopHandleBatches();
        handleRemainingBatches();
    }

    private void loopHandleBatches() {
        while (!closed) {
            try {
                ProducerBatch b = batches.take();
                handle(b);
            } catch (InterruptedException e) {
                LOGGER.info("The batch handler has been interrupted");
            }
        }
    }


    private void handle(ProducerBatch batch) {
        try {
            batch.fireCallbacksAndSetFutures();
            LOGGER.info("batch={} batchGroupName={} has execute callBack successfully!", batch.getBatchId(), batch.getGroupName());
        } catch (Throwable t) {
            LOGGER.error("Failed to handle batch, batchId={} batchGroupName={}, e=", batch.getBatchId(), batch.getGroupName(), t);
        } finally {
            batchCount.decrementAndGet();
            memoryController.release(batch.getCurBatchSizeInBytes());
        }
    }

    private void handleRemainingBatches() {
        List<ProducerBatch> remainingBatches = new ArrayList<>();
        batches.drainTo(remainingBatches);
        for (ProducerBatch b : remainingBatches) {
            handle(b);
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
        interrupt();
    }


}
