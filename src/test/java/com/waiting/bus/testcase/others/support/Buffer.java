package com.waiting.bus.testcase.others.support;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition ;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Buffer {
    private final Queue<Integer> queue = new LinkedList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    private final int capacity;
    private int count;

    public Buffer(int capacity) {
        this.capacity = capacity;
    }

    public void put(int value) throws InterruptedException {
        lock.lock();
        try {
            while (count == capacity) { // 如果缓冲区已满，则等待
                notFull.await();
            }

            queue.add(value);
            count++;
            notEmpty.signal(); // 唤醒等待的消费者线程
        } finally {
            lock.unlock();
        }
    }

    public int get() throws InterruptedException {
        lock.lock();
        try {
            while (count == 0) { // 如果缓冲区为空，则等待
                notEmpty.await();
            }

            int value = queue.remove();
            count--;
            notFull.signal(); // 唤醒等待的生产者线程
            return value;
        } finally {
            lock.unlock();
        }
    }
}
