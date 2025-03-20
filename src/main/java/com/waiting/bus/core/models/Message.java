package com.waiting.bus.core.models;

import java.util.Map;

/**
 * @author jianzhang
 * @date 2024/2/5
 */
public class Message<T> {

    private String groupName;

    private T message;

    private Map<String, Object> ext;

    public Message(T message, Map<String, Object> ext) {
        this.message = message;
        this.ext = ext;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }

    public Map<String, Object> getExt() {
        return ext;
    }

    public void setExt(Map<String, Object> ext) {
        this.ext = ext;
    }


    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String toString() {
        return "Message{" +
                "message='" + message + '\'' +
                ", ext=" + ext +
                '}';
    }
}
