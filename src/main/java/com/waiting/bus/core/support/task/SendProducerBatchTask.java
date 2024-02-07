package com.waiting.bus.core.support.task;

import com.google.common.math.LongMath;
import com.waiting.bus.constant.Errors;
import com.waiting.bus.core.containers.ProducerBatch;
import com.waiting.bus.core.containers.RetryQueue;
import com.waiting.bus.core.models.Attempt;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.constant.MessageProcessResultEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;

/**
 * @author jianzhang
 * @date 2024/2/5
 */

public class SendProducerBatchTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendProducerBatchTask.class);
    private final BlockingQueue<ProducerBatch> successQueue;

    private final BlockingQueue<ProducerBatch> failureQueue;

    private final RetryQueue retryQueue;

    private final ProducerConfig producerConfig;

    private final Function<List<Message>, MessageProcessResultEnum> sendFunction;

    private final ProducerBatch batch;

    public SendProducerBatchTask(ProducerBatch producerBatch, ProducerConfig producerConfig, Function<List<Message>, MessageProcessResultEnum> sendFunction,
                                 RetryQueue retryQueue, BlockingQueue<ProducerBatch> successQueue, BlockingQueue<ProducerBatch> failureQueue) {
        this.batch = producerBatch;
        this.producerConfig = producerConfig;
        this.sendFunction = sendFunction;
        this.retryQueue = retryQueue;
        this.successQueue = successQueue;
        this.failureQueue = failureQueue;
    }

    @Override
    public void run() {
        try {
            sendProducerBatch();
        } catch (InterruptedException e) {
            LOGGER.error("SendProducerBatchTask interrupted", e);
        }
    }

    private void sendProducerBatch() throws InterruptedException {
        try {

            MessageProcessResultEnum messageProcessResultEnum = sendFunction.apply(batch.getMessageList());

            if (messageProcessResultEnum != null && Objects.equals(MessageProcessResultEnum.RETRY.getCode(), messageProcessResultEnum.getCode())) {
                retryIfNeeded(Errors.BIZ_NEED_RETRY, "biz handle message need retry");
            }

            if (messageProcessResultEnum != null && Objects.equals(MessageProcessResultEnum.FAIL.getCode(), messageProcessResultEnum.getCode())) {
                retryIfNeeded(Errors.BIZ_EXECUTE_FAILED, "biz handle message failed");
            }

        } catch (Exception e) {
            retryIfNeeded(Errors.PRODUCER_EXCEPTION, e.getMessage());
        }
        Attempt attempt = new Attempt(true, "", "", System.currentTimeMillis());
        batch.appendAttempt(attempt);
        successQueue.put(batch);
    }

    private long calculateRetryBackoffMs() {
        int retry = batch.getRetries();
        long retryBackoffMs = producerConfig.getBaseRetryBackoffMs() * LongMath.pow(2, retry);
        if (retryBackoffMs <= 0) {
            retryBackoffMs = producerConfig.getMaxRetryBackoffMs();
        }
        return Math.min(retryBackoffMs, producerConfig.getMaxRetryBackoffMs());
    }


    public void retryIfNeeded(String retryCode, String errMsg) throws InterruptedException {
        Attempt attempt = new Attempt(false, retryCode, errMsg, System.currentTimeMillis());
        batch.appendAttempt(attempt);

        boolean canRetry = batch.getRetries() < producerConfig.getRetries();

        if (canRetry) {
            long retryBackoffMs = calculateRetryBackoffMs();
            batch.setNextRetryMs(System.currentTimeMillis() + retryBackoffMs);
            retryQueue.put(batch);
            LOGGER.info("retry batch, batchId={}, retryCount={}", batch.getBatchId(), batch.getRetries());
        } else {
            failureQueue.put(batch);
        }
    }

}
