package com.waiting.bus.testcase;

import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.constant.MessageProcessResultEnum;
import com.waiting.bus.core.MessageProducer;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.exceptions.ProducerException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author jianzhang
 * @date 2024/2/20
 */
public class QuickStartDemo {

    public static void main(String[] args) {
        ProducerConfig producerConfig = new ProducerConfig();
        // 设定攒批达到20条以上的时候再执行业务逻辑
        producerConfig.setBatchCountThreshold(20);
        // 设定攒批已经达到100s的时候再执行业务逻辑
        producerConfig.setLingerMs(100_000);

        // producerConfig.setRetries(2); 可以指定重试次数
        MessageProducer messageProducer = getMessageProducer(producerConfig);

        //模拟接收消息，进行攒批，到达攒批上限（触发攒批最大长度或者攒批总时长超过100s时）的时候会执行对应的业务逻辑
        sendMessage(messageProducer);
    }

    private static MessageProducer getMessageProducer(ProducerConfig producerConfig) {
        return new MessageProducer(producerConfig, (List<Message> arr) -> {

            System.out.println("执行消息处理逻辑 当前时间" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
                    + " 当前批次大小:" + arr.size());

            // do your business here

            return MessageProcessResultEnum.SUCCESS;
        });
    }


    private static void sendMessage(MessageProducer messageProducer) {
        Thread t1 = new Thread(() -> {
            while (true) {
                try {
                    List<Message> messageList = new ArrayList<>();
                    int num = new Random().nextInt(10);
                    for (int i = 0; i < num; i++) {
                        String message = "模拟消息-" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()) + "-" + i;
                        messageList.add(new Message(message, null));
                    }
                    // 内存攒批
                    messageProducer.send(null, messageList);
                    sleep(new Random().nextInt(1000));

                } catch (InterruptedException | ProducerException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t1.start();
    }

    private static void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
