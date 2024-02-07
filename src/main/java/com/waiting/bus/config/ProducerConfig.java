package com.waiting.bus.config;


public class ProducerConfig {

    public static final int DEFAULT_TOTAL_SIZE_IN_BYTES = 100 * 1024 * 1024;

    public static final long DEFAULT_MAX_BLOCK_MS = 60 * 1000L;

    public static final int DEFAULT_IO_THREAD_COUNT =
            Math.max(Runtime.getRuntime().availableProcessors(), 1);

    public static final int DEFAULT_BATCH_SIZE_THRESHOLD_IN_BYTES = 512 * 1024;

    public static final int MAX_BATCH_SIZE_IN_BYTES = 8 * 1024 * 1024;

    public static final int DEFAULT_BATCH_COUNT_THRESHOLD = 4096;

    public static final int MAX_BATCH_COUNT = 40960;

    public static final int DEFAULT_LINGER_MS = 2000;

    public static final int LINGER_MS_LOWER_LIMIT = 100;

    public static final int DEFAULT_RETRIES = 10;

    public static final long DEFAULT_BASE_RETRY_BACKOFF_MS = 100L;

    public static final long DEFAULT_MAX_RETRY_BACKOFF_MS = 50 * 1000L;


    private int totalSizeInBytes = DEFAULT_TOTAL_SIZE_IN_BYTES;

    private long maxBlockMs = DEFAULT_MAX_BLOCK_MS;

    private int ioThreadCount = DEFAULT_IO_THREAD_COUNT;

    private int batchSizeThresholdInBytes = DEFAULT_BATCH_SIZE_THRESHOLD_IN_BYTES;

    private int batchCountThreshold = DEFAULT_BATCH_COUNT_THRESHOLD;

    private int lingerMs = DEFAULT_LINGER_MS;

    private int retries = DEFAULT_RETRIES;

    private int maxReservedAttempts = DEFAULT_RETRIES + 1;

    private long baseRetryBackoffMs = DEFAULT_BASE_RETRY_BACKOFF_MS;

    private long maxRetryBackoffMs = DEFAULT_MAX_RETRY_BACKOFF_MS;


    /**
     * @return 攒批最大可使用内存
     */
    public int getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    /**
     * 设置攒批最大可使用内存  单位为Byte
     */
    public void setTotalSizeInBytes(int totalSizeInBytes) {
        if (totalSizeInBytes <= 0) {
            throw new IllegalArgumentException(
                    "totalSizeInBytes must be greater than 0, got " + totalSizeInBytes);
        }
        this.totalSizeInBytes = totalSizeInBytes;
    }

    /**
     * @return send方法等待获取内存的最大等待时间 单位毫秒
     */
    public long getMaxBlockMs() {
        return maxBlockMs;
    }

    /**
     * 设置send方法等待获取内存的最大等待时间 单位毫秒
     */
    public void setMaxBlockMs(long maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }

    /**
     * @return 发送消息线程池的线程数
     */
    public int getIoThreadCount() {
        return ioThreadCount;
    }

    /**
     * 设置处理消息线程池的线程数
     */
    public void setIoThreadCount(int ioThreadCount) {
        if (ioThreadCount <= 0) {
            throw new IllegalArgumentException(
                    "ioThreadCount must be greater than 0, got " + ioThreadCount);
        }
        this.ioThreadCount = ioThreadCount;
    }

    /**
     * @return 单个批次占据内存占用上限 达到这个限制就会直接进行消息发送，不再攒批
     */
    public int getBatchSizeThresholdInBytes() {
        return batchSizeThresholdInBytes;
    }

    /**
     * 设置 单个批次占据内存占用上限 达到这个限制就会直接进行消息发送，不再攒批
     */
    public void setBatchSizeThresholdInBytes(int batchSizeThresholdInBytes) {
        if (batchSizeThresholdInBytes < 1 || batchSizeThresholdInBytes > MAX_BATCH_SIZE_IN_BYTES) {
            throw new IllegalArgumentException(
                    String.format(
                            "batchSizeThresholdInBytes must be between 1 and %d, got %d",
                            MAX_BATCH_SIZE_IN_BYTES, batchSizeThresholdInBytes));
        }
        this.batchSizeThresholdInBytes = batchSizeThresholdInBytes;
    }


    /**
     * @return 获取单个批次最多承载数量 达到这个数目就会直接进行消息发送，不再攒批
     */
    public int getBatchCountThreshold() {
        return batchCountThreshold;
    }


    /**
     * 设置单个批次最多承载数量 达到这个数目就会直接进行消息发送，不再攒批
     */
    public void setBatchCountThreshold(int batchCountThreshold) {
        if (batchCountThreshold < 1 || batchCountThreshold > MAX_BATCH_COUNT) {
            throw new IllegalArgumentException(
                    String.format(
                            "batchCountThreshold must be between 1 and %d, got %d",
                            MAX_BATCH_COUNT, batchCountThreshold));
        }
        this.batchCountThreshold = batchCountThreshold;
    }

    /**
     * @return 一个批次最大等待时间
     */
    public int getLingerMs() {
        return lingerMs;
    }

    /**
     * 设置一个批次最大等待时间
     */
    public void setLingerMs(int lingerMs) {
        if (lingerMs < LINGER_MS_LOWER_LIMIT) {
            throw new IllegalArgumentException(
                    String.format(
                            "lingerMs must be greater than or equal to %d, got %d",
                            LINGER_MS_LOWER_LIMIT, lingerMs));
        }
        this.lingerMs = lingerMs;
    }

    /**
     * @return 重试次数，低于这个次数的消息会自动加入重试队列
     */
    public int getRetries() {
        return retries;
    }

    /**
     * 设置重试次数
     */
    public void setRetries(int retries) {
        this.retries = retries;
    }


    /**
     * 重试结果保存的最多个数
     *
     * @return 重试结果保存的最多个数
     */
    public int getMaxReservedAttempts() {
        return maxReservedAttempts;
    }


    public void setMaxReservedAttempts(int maxReservedAttempts) {
        if (maxReservedAttempts <= 0) {
            throw new IllegalArgumentException("maxReservedAttempts must be greater than 0, got " + maxReservedAttempts);
        }
        this.maxReservedAttempts = maxReservedAttempts;
    }


    public long getBaseRetryBackoffMs() {
        return baseRetryBackoffMs;
    }


    public void setBaseRetryBackoffMs(long baseRetryBackoffMs) {
        if (baseRetryBackoffMs <= 0) {
            throw new IllegalArgumentException("baseRetryBackoffMs must be greater than 0, got " + baseRetryBackoffMs);
        }
        this.baseRetryBackoffMs = baseRetryBackoffMs;
    }

    public long getMaxRetryBackoffMs() {
        return maxRetryBackoffMs;
    }

    public void setMaxRetryBackoffMs(long maxRetryBackoffMs) {
        if (maxRetryBackoffMs <= 0) {
            throw new IllegalArgumentException("maxRetryBackoffMs must be greater than 0, got " + maxRetryBackoffMs);
        }
        this.maxRetryBackoffMs = maxRetryBackoffMs;
    }

}
