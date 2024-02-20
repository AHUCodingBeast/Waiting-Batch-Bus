package com.waiting.bus.core.containers;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.core.ext.Callback;
import com.waiting.bus.core.models.Attempt;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.core.models.Result;
import com.waiting.bus.core.models.Thunk;
import com.waiting.bus.exceptions.ResultFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author jianzhang
 * @date 2024/1/31
 */
public class ProducerBatch implements Delayed {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerBatch.class);

    private final List<Message> messageList = new ArrayList<Message>();

    private final List<Thunk> thunks = new ArrayList<Thunk>();

    private int curBatchSizeInBytes;

    private int curBatchCount;

    private final EvictingQueue<Attempt> reservedAttempts;

    private int attemptCount;

    private final long createdMs;

    private final int batchSizeThresholdInBytes;

    private final int batchCountThreshold;


    private long nextRetryMs;


    private String groupName;

    private String batchId;

    /**
     * 构建一个攒批
     *
     * @param batchSizeThresholdInBytes 这个批次占据的最大内存
     * @param batchCountThreshold       这个批次最多持有的消息上限
     * @param maxReservedAttempts       这个批次最多拥有的重试次数
     * @param groupName                 当前攒批所属的组名称
     */
    public ProducerBatch(int batchSizeThresholdInBytes, int batchCountThreshold, int maxReservedAttempts, String groupName) {
        this.createdMs = System.currentTimeMillis();
        this.batchSizeThresholdInBytes = batchSizeThresholdInBytes;
        this.batchCountThreshold = batchCountThreshold;
        this.curBatchCount = 0;
        this.curBatchSizeInBytes = 0;
        this.reservedAttempts = EvictingQueue.create(maxReservedAttempts);
        this.attemptCount = 0;
        this.groupName = groupName;
        this.batchId = this.groupName + "_" + System.currentTimeMillis() + "_" + new Random().nextInt(1000);
    }

    /**
     * 往当前攒批中继续添加消息
     *
     * @param items       需要添加的消息
     * @param sizeInBytes 消息占据的内存
     * @param callback    当前追加的数据的执行回调
     * @return 这批追加的消息的处理结果
     */
    public ListenableFuture<Result> tryAppend(List<Message> items, int sizeInBytes, Callback callback) {
        if (!hasRoomFor(sizeInBytes, items.size())) {
            return null;
        } else {
            SettableFuture<Result> future = SettableFuture.create();
            messageList.addAll(items);
            thunks.add(new Thunk(callback, future));
            curBatchCount += items.size();
            curBatchSizeInBytes += sizeInBytes;
            return future;
        }
    }

    /**
     * 往当前攒批中继续添加消息 (不回调) 不关心结果返回
     *
     * @param items       需要添加的消息
     * @param sizeInBytes 消息占据的内存
     * @return 是否成功
     */
    public boolean tryAppend(List<Message> items, int sizeInBytes) {
        if (hasRoomFor(sizeInBytes, items.size())) {
            curBatchCount += items.size();
            curBatchSizeInBytes += sizeInBytes;
            messageList.addAll(items);
            return true;
        }
        return false;
    }


    public boolean isMeetSendCondition() {
        return curBatchSizeInBytes >= batchSizeThresholdInBytes || curBatchCount >= batchCountThreshold;
    }

    public void fireCallbacksAndSetFutures() {
        List<Attempt> attempts = new ArrayList<Attempt>(reservedAttempts);
        Attempt attempt = Iterables.getLast(attempts);
        Result result = new Result(attempt.isSuccess(), attempts, attemptCount);
        fireCallbacks(result);
        setFutures(result);
    }

    private void fireCallbacks(Result result) {
        for (Thunk thunk : thunks) {
            try {
                if (thunk.callback != null) {
                    thunk.callback.onCompletion(result);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to execute user-provided callback, groupName={}, e=", groupName, e);
            }
        }
    }

    private void setFutures(Result result) {
        for (Thunk thunk : thunks) {
            try {
                if (result.isSuccessful()) {
                    thunk.future.set(result);
                } else {
                    thunk.future.setException(new ResultFailedException(result));
                }
            } catch (Exception e) {
                LOGGER.error("Failed to set future, groupName={}, e=", groupName, e);
            }
        }
    }

    private boolean hasRoomFor(int sizeInBytes, int count) {
        return curBatchSizeInBytes + sizeInBytes <= ProducerConfig.MAX_BATCH_SIZE_IN_BYTES
                && curBatchCount + count <= ProducerConfig.MAX_BATCH_COUNT;
    }

    public int getCurBatchSizeInBytes() {
        return curBatchSizeInBytes;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(nextRetryMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return (int) (nextRetryMs - ((ProducerBatch) o).getNextRetryMs());
    }

    public void setNextRetryMs(long nextRetryMs) {
        this.nextRetryMs = nextRetryMs;
    }

    public long getNextRetryMs() {
        return nextRetryMs;
    }

    public List<Message> getMessageList() {
        return messageList;
    }

    public void appendAttempt(Attempt attempt) {
        reservedAttempts.add(attempt);
        this.attemptCount++;
    }


    public int getRetries() {
        return Math.max(0, attemptCount - 1);
    }


    public long remainingMs(long nowMs, long lingerMs) {
        return lingerMs - Math.max(0, nowMs - createdMs);
    }

    public long getCreatedMs() {
        return createdMs;
    }


    public String getGroupName() {
        return groupName;
    }

    public String getBatchId() {
        return batchId;
    }


}
