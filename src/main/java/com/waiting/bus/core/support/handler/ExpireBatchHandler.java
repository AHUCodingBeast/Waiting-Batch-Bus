package com.waiting.bus.core.support.handler;

import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.core.containers.ProducerBatch;
import com.waiting.bus.core.containers.ProducerBatchContainer;
import com.waiting.bus.core.containers.RetryQueue;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.constant.MessageProcessResultEnum;
import com.waiting.bus.core.support.task.SendProducerBatchTask;
import com.waiting.bus.core.support.utils.IOThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

/**
 * @author jianzhang
 * @date 2024/2/6
 */
public class ExpireBatchHandler extends Thread {


    private final ProducerConfig producerConfig;

    private final RetryQueue retryQueue;

    private final BlockingQueue<ProducerBatch> successQueue;

    private final BlockingQueue<ProducerBatch> failureQueue;

    private final IOThreadPool ioThreadPool;

    private volatile boolean closed;

    private final Function<List<Message>, MessageProcessResultEnum> messageProcessFunction;

    private final ProducerBatchContainer producerBatchContainer;

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpireBatchHandler.class);

    public ExpireBatchHandler(
            String name,
            ProducerConfig producerConfig,
            Function<List<Message>, MessageProcessResultEnum> messageProcessFunction,
            ProducerBatchContainer producerBatchContainer,
            RetryQueue retryQueue,
            BlockingQueue<ProducerBatch> successQueue,
            BlockingQueue<ProducerBatch> failureQueue,
            IOThreadPool ioThreadPool) {
        setDaemon(true);
        setUncaughtExceptionHandler(
                (t, e) -> LOGGER.error("Uncaught error in thread, name={}, e=", name, e));
        this.producerConfig = producerConfig;
        this.messageProcessFunction = messageProcessFunction;
        this.producerBatchContainer = producerBatchContainer;
        this.retryQueue = retryQueue;
        this.successQueue = successQueue;
        this.failureQueue = failureQueue;
        this.ioThreadPool = ioThreadPool;
        this.closed = false;
    }

    @Override
    public void run() {
        // 循环检查有无过期攒批
        loopHandleExpireBatch();
        // 提交未完成的批次
        submitIncompleteBatches();
    }

    private void loopHandleExpireBatch() {
        while (!closed) {
            try {
                List<ProducerBatch> expiredBatches1 = producerBatchContainer.getExpiredBatches();
                List<ProducerBatch> expiredBatches2 = retryQueue.getExpiredBatches(1000);
                expiredBatches1.addAll(expiredBatches2);
                for (ProducerBatch b : expiredBatches1) {
                    ioThreadPool.submit(new SendProducerBatchTask(b, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue));
                }
            } catch (Exception e) {
                LOGGER.error("loopHandleExpireBatch Exception, e=", e);
            }
        }
    }


    private void submitIncompleteBatches() {
        List<ProducerBatch> incompleteBatches = producerBatchContainer.getRemainingBatches();
        for (ProducerBatch b : incompleteBatches) {
            ioThreadPool.submit(new SendProducerBatchTask(b, producerConfig, messageProcessFunction, retryQueue, successQueue, failureQueue));
        }
    }


    public void close() {
        this.closed = true;
        interrupt();
    }

}
