package com.waiting.bus.constant;

/**
 * @author jianzhang
 * @date 2024/2/6
 */
public enum MessageProcessResultEnum {

    FAIL(0, "fail"),
    SUCCESS(1, "success"),

    RETRY(2, "retry");



    private Integer code;

    private String message;

    MessageProcessResultEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }
    public Integer getCode() {
        return code;
    }
    public String getMessage() {
        return message;

    }
}
