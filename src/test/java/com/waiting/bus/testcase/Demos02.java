package com.waiting.bus.testcase;

import com.google.common.util.concurrent.ListenableFuture;
import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.constant.MessageProcessResultEnum;
import com.waiting.bus.core.MessageProducer;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.core.models.Result;
import com.waiting.bus.exceptions.ProducerException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author jianzhang
 * @date 2024/2/6
 *
 * 单批次 设定最低数目发送
 */
public class Demos02 {


    public static void main(String[] args) {

        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setBatchCountThreshold(100);
        producerConfig.setLingerMs(60000);

        MessageProducer messageProducer = getMessageProducer(producerConfig);
        // 直接往生产者中投放内容即可 会按照制定的规则进行发送
        String batchId = "batch-001";

        while (true) {
            try {
                List<Message> messageList = new ArrayList<>();
                int num = new Random().nextInt(100);
                for (int i = 0; i < num; i++) {
                    messageList.add(new Message("模拟消息-" + i, null));
                }
                ListenableFuture<Result> send = messageProducer.send("batch-001", messageList, null);

                sleep(new Random().nextInt(2000));

            } catch (InterruptedException | ProducerException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static MessageProducer getMessageProducer(ProducerConfig producerConfig) {
        // 实现自己的消息处理逻辑
        return new MessageProducer(producerConfig, (List<Message> arr) -> {
            System.out.println("准备发送消息 当前时间" +
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()) +
                    " 当前批次大小:" + arr.size());

            sleep(1000);

            return MessageProcessResultEnum.SUCCESS;
        });
    }

    private static void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
