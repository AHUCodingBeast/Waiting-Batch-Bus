package com.waiting.bus.core.models;


public class Attempt {

    private final boolean success;

    private final String errorCode;

    private final String errorMessage;

    private final long timestampMs;

    public Attempt(boolean success, String errorCode, String errorMessage, long timestampMs) {
        this.success = success;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.timestampMs = timestampMs;
    }

    /**
     * @return Whether the attempt was successful. If true, then the batch has been confirmed by the
     * backend.
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * @return Error code associated with this attempt. Empty string if no error (i.e. successful).
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * @return Error message associated with this attempt. Empty string if no error (i.e. successful).
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * @return The time when this attempt happened.
     */
    public long getTimestampMs() {
        return timestampMs;
    }

    @Override
    public String toString() {
        return "Attempt{"
                + "success="
                + success
                + '\''
                + ", errorCode='"
                + errorCode
                + '\''
                + ", errorMessage='"
                + errorMessage
                + '\''
                + ", timestampMs="
                + timestampMs
                + '}';
    }
}
