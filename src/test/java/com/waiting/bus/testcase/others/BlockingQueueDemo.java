package com.waiting.bus.testcase.others;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author jianzhang
 * @date 2024/2/7
 * <p>
 * 解释下blockingQueue的使用
 * BlockingQueue是Java并发包（java.util.concurrent）中提供的一个接口，它扩展了标准的Queue接口，并增加了阻塞操作的支持。
 * 这意味着当队列为空时，尝试从中获取元素的操作将会被阻塞，直到有其他线程向队列中添加元素为止；
 * 同样，当队列已满时，尝试向队列中添加元素的操作也会被阻塞，直到有其他线程从队列中移除了元素为止。
 * 这种机制使得BlockingQueue非常适合用于多线程环境下的生产者-消费者模型。
 * <p>
 * 创建BlockingQueue实例：BlockingQueue提供了多种实现类，如ArrayBlockingQueue、LinkedBlockingQueue、PriorityBlockingQueue等。
 * 你可以根据实际需求选择合适的实现类，并通过构造函数创建BlockingQueue实例。
 * 向队列中添加元素：可以使用put()方法将元素添加到队列中。如果队列已满，该方法将会阻塞，直到有足够的空间容纳新的元素为止。
 * 从队列中获取元素：可以使用take()方法从队列中获取元素。如果队列为空，该方法将会阻塞，直到有其他线程向队列中添加了元素为止。
 * 非阻塞操作：除了阻塞操作外，BlockingQueue还提供了非阻塞版本的插入和获取方法，如offer()、poll()等。
 * 这些方法在无法立即完成操作时不会阻塞，而是返回一个布尔值或空值，表示操作是否成功。
 */
public class BlockingQueueDemo {
    public static void main(String[] args) throws Exception{
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(5);

        // 创建两个线程，分别作为生产者和消费者
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    // 将数字i添加到队列中
                    String data = "something-" + i;
                    queue.put(data);
                    System.out.println("Produced: " + data);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread consumer = new Thread(() -> {
            while (true) {
                try {
                    // 从队列中获取并打印一个数字
                    String num = queue.take();
                    System.out.println("Consumed: " + num);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // 启动生产者和消费者线程
        producer.start();
        consumer.start();

        // 等待生产者线程结束
        producer.join();

        consumer.join();
        // 停止消费者线程
        //consumer.interrupt();
    }
}
