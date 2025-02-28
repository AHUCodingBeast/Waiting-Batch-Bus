package com.waiting.bus.testcase;

import com.waiting.bus.config.ProducerConfig;
import com.waiting.bus.constant.MessageProcessResultEnum;
import com.waiting.bus.core.MessageProducer;
import com.waiting.bus.core.models.Message;
import com.waiting.bus.exceptions.ProducerException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

public class BugTest {

    public static void main(String[] args) throws ProducerException, InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig();
        // 设定攒批达到20条以上的时候再执行业务逻辑
        producerConfig.setBatchCountThreshold(100);
        // 设定攒批已经达到100s的时候再执行业务逻辑
        producerConfig.setLingerMs(10000);
        producerConfig.setBaseRetryBackoffMs(3000);
        producerConfig.setRetries(3);

        // producerConfig.setRetries(2); 可以指定重试次数
        MessageProducer messageProducer = getMessageProducer(producerConfig);

        //模拟接收消息，进行攒批，到达攒批上限（触发攒批最大长度或者攒批总时长超过100s时）的时候会执行对应的业务逻辑
        sendMessage(messageProducer);
        messageProducer.close();
    }

    private static MessageProducer getMessageProducer(ProducerConfig producerConfig) {
        return new MessageProducer(producerConfig, (List<Message> arr) -> {
            arr.forEach(message -> message.getExt().put("retry", (Integer) message.getExt().getOrDefault("retry", 0) + 1));
            System.out.println("执行消息处理逻辑 当前时间" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
                    + " 当前批次大小:" + arr.size()+"重试次数:"+arr.get(0).getExt().get("retry"));
            // do your business here
            sleep(new Random().nextInt(1000));
            return MessageProcessResultEnum.RETRY;
        });
    }

    private static void sendMessage(MessageProducer messageProducer) throws InterruptedException {
        Thread t1 = new Thread(() -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    Thread.sleep(new Random().nextInt(200));
                    String message = "模拟消息-" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()) + "-" + i;
                    messageProducer.send(null, new Message(message, new HashMap<>()));
                }
            } catch (Exception e) {

            }
        });
        t1.start();
        t1.join();
    }

    private static void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
