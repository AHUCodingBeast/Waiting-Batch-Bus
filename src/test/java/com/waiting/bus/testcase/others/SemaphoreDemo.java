package com.waiting.bus.testcase.others;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author jianzhang
 * @date 2024/2/7
 */
public class SemaphoreDemo {

    public static void main(String[] args) {
        // 可以通过Semaphore来限制同时运行的任务数量
        Semaphore semaphore = new Semaphore(2);
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Task(semaphore), "Task-" + i);
            thread.start();
        }
    }


    private static class Task implements Runnable {
        private final Semaphore semaphore;

        public Task(Semaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public void run() {
            try {
                semaphore.acquire(); // 请求许可证

                System.out.println(Thread.currentThread().getName() + " 开始执行任务");

                TimeUnit.SECONDS.sleep(1); // 模拟任务耗时

                System.out.println(Thread.currentThread().getName() + " 完成任务");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                semaphore.release(); // 归还许可证
            }
        }
    }
}
