package com.waiting.bus.core.models;

import java.util.Map;

/**
 * @author jianzhang
 * @date 2024/2/5
 */
public class Message {

    private String groupName;

    private String message;

    private Map<String,String> ext;

    public Message(String message, Map<String, String> ext) {
        this.message = message;
        this.ext = ext;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, String> getExt() {
        return ext;
    }

    public void setExt(Map<String, String> ext) {
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
