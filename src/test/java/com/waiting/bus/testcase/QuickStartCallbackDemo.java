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
import java.util.*;

/**
 * @author jianzhang
 * @date 2024/2/20
 */
public class QuickStartCallbackDemo {

    public static void main(String[] args) {
        ProducerConfig producerConfig = new ProducerConfig();
        // 设定攒批达到20条以上的时候再执行业务逻辑
        producerConfig.setBatchCountThreshold(40);
        // 设定攒批已经达到100s的时候再执行业务逻辑
        producerConfig.setLingerMs(50_00);

        // producerConfig.setRetries(2); 可以指定重试次数
        MessageProducer messageProducer = getMessageProducer(producerConfig);
        sendMessageWithCallBack(messageProducer);
    }

    private static MessageProducer getMessageProducer(ProducerConfig producerConfig) {
        return new MessageProducer(producerConfig, (List<Message> arr) -> {

            System.out.println("执行消息处理逻辑 当前时间" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
                    + " 当前批次大小:" + arr.size());

            // do your business here
            sleep(new Random().nextInt(5000));

            return MessageProcessResultEnum.SUCCESS;
        });
    }


    private static void sendMessageWithCallBack(MessageProducer messageProducer) {
        Thread t1 = new Thread(() -> {
            int seq = 1;
            while (true) {
                try {
                    Message msg = new Message("模拟消息-" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()), null);
                    String appendSequenceNo = "追加攒批-当前攒批序号" + "-" + seq + "-" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
                    seq++;
                    // 内存攒批
                    ListenableFuture<Result> listenableFuture = messageProducer.send(null, msg, (result) -> {
                        if (result.isSuccessful()) {
                            System.out.println( appendSequenceNo + "-消费成功");
                        } else {
                            System.out.println( appendSequenceNo + "-消费失败");
                        }
                    });
                    // 也可以直接为 listenableFuture 绑定监听 了解每次追加的消息的消费结果
                    sleep(new Random().nextInt(1000));
                } catch (InterruptedException | ProducerException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t1.start();
    }

    private static void sendMessage(MessageProducer messageProducer) {
        Thread t1 = new Thread(() -> {
            while (true) {
                try {
                    int num = new Random().nextInt(10);
                    for (int i = 0; i < num; i++) {
                        Thread.sleep(new Random().nextInt(200));
                        String message = "模拟消息-" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()) + "-" + i;
                        messageProducer.send(null, new Message(message, null),(result) -> {
                            if (result.isSuccessful()) {
                                System.out.println( message + "-消费成功");
                            } else {
                                System.out.println( message + "-消费失败");
                            }
                        });
                    }
                    // 内存攒批
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
