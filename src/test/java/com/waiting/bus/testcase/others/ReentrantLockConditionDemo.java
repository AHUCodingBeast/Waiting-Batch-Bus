package com.waiting.bus.testcase.others;

import com.waiting.bus.testcase.others.support.Buffer;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jianzhang
 * @date 2024/2/18
 */
public class ReentrantLockConditionDemo {
    public static void main(String[] args) throws Exception {
    }


    private void wrongCall() throws Exception{
        // 在调用notEmpty.await()之前，没有通过lock()方法获取到锁。
        // await()方法必须在持有锁的情况下被调用，否则就会抛出这个异常。
        Lock lock = new ReentrantLock();
        Condition notEmpty = lock.newCondition();
        // IllegalMonitorStateException
        notEmpty.await();
        System.out.println("finish");
    }



    private static void productAndConsume() {
        /**
         * ReentrantLock是Java并发包中的一个可重入互斥锁，它提供了一种替代synchronized关键字的方式。与synchronized不同的是，
         * ReentrantLock提供了更多的高级特性，如公平锁、非公平锁、可中断锁等待、超时锁等待以及条件变量等。
         *
         * Condition是ReentrantLock的一个接口，它提供了与synchronized块中的wait()和notifyAll()类似的功能，
         * 但具有更高的灵活性。Condition可以让你为每个锁定对象创建多个条件变量，每个条件变量都有自己的等待队列。
         *
         * 这样，你可以根据具体的需求来控制线程之间的协作。
         *
         * 在这个示例中，我们创建了一个容量为10的缓冲区，并分别创建了一个生产者线程和一个消费者线程。生产者线程生成数字并将它们放入缓冲区，
         * 而消费者线程从缓冲区取出数字并消费它们。当缓冲区满时，生产者线程会等待，直到消费者线程消费了一些数字并释放了空间。
         * 同样，当缓冲区空时，消费者线程也会等待，直到生产者线程向缓冲区添加了一些数字。
         * ReentrantLock和Condition在这里起到了关键作用，确保了线程间的正确同步和协作。
         */

        Buffer buffer = new Buffer(10);

        Thread producer = new Thread(() -> {
            for (int i = 0; i < 20; i++) {
                try {
                    buffer.put(i);
                    System.out.println("Produced: " + i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread consumer = new Thread(() -> {
            for (int i = 0; i < 20; i++) {
                try {
                    int value = buffer.get();
                    System.out.println("Consumed: " + value);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        producer.start();
        consumer.start();
    }

}
