package com.waiting.bus.core.support.utils;

import com.waiting.bus.core.models.Message;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import java.util.List;

/**
 * @author jianzhang
 * @date 2024/1/31
 */
public class DataSizeCalculator {

    public static int calculateSize(List<Message> messageList) {
        return messageList.size();
    }

    public static int calculateMessageByteSize(List<Message> messageList) {
        if (messageList == null) {
            return 0;
        }
        int messagelength = 0;
        for (Message message : messageList) {
            messagelength += ObjectSizeCalculator.getObjectSize(message);
        }
        return messagelength;
    }

}
