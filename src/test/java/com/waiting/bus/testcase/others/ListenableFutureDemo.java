package com.waiting.bus.testcase.others;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.*;

/**
 * @author jianzhang
 * @date 2024/2/7
 */
public class ListenableFutureDemo {

    public static void main(String[] args)  {
        ListenableFutureDemo listenableFutureDemo = new ListenableFutureDemo();
        listenableFutureDemo.useListenableFuture();
    }

    private void futureDemo() throws Exception{
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> submit = executor.submit(() -> {
            try {
                Thread.sleep(1000);
                return "Async Task Result";
            } catch (Exception e) {
                throw e;
            } finally {
                executor.shutdown();
            }
        });
        String s = submit.get();
        System.out.println("Async Task Result: " + s);

    }


    private ListenableFuture<String> simulateAsyncTask() {
        SettableFuture<String> future = SettableFuture.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        executor.execute(() -> {
            try {
                Thread.sleep(1000); // 模拟耗时操作
                future.set("Async Task Result");
            } catch (Exception e) {
                future.setException(e);
            } finally {
                executor.shutdown();
            }
        });

        return future;
    }

    private void handleAsyncResult(ListenableFuture<String> asyncResult) throws ExecutionException, InterruptedException {
        String result = asyncResult.get(); // 获取异步任务的结果
        System.out.println("Async Task Result: " + result);

        // 在这里处理异步任务的结果或执行其他操作
    }

    public void useListenableFuture()  {
        ListenableFuture<String> asyncTask = simulateAsyncTask();

        /**
         * 可扩展性：ListenableFuture允许你添加多个监听器，这意味着你可以根据不同的需求定义多个处理异步任务完成后的操作。
         * 相比之下，Future仅提供了获取结果、取消任务和判断任务是否已完成的基本功能。
         *
         * 非阻塞性：使用ListenableFuture，你可以在不阻塞当前线程的情况下注册监听器。
         * 这意味着你的应用程序可以在等待异步任务完成的同时继续执行其他操作，提高了程序的响应速度和并发性能。
         *
         * 灵活性：ListenableFuture可以通过transform()和transformAsync()方法对异步任务的结果进行进一步处理。
         * 这些方法允许你在异步任务完成后应用函数式编程风格的操作，如映射、过滤等，从而简化代码并提高可读性。
         *
         * 易于集成：由于ListenableFuture是Google Guava库的一部分，因此它与其他Guava工具（如Futures类）很好地集成在一起。
         * 这使得你可以利用Guava提供的各种实用工具和优化来处理异步任务。
         *
         * 更好的错误处理：ListenableFuture允许你在监听器中捕获和处理异常，而不是在主线程中抛出异常。
         * 这有助于防止因未被捕获的异常而导致的应用程序崩溃，并使你能够更灵活地控制错误处理流程。
         */

        asyncTask.addListener(() -> {
            try {
                handleAsyncResult(asyncTask);
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }, Runnable::run);

        asyncTask.addListener(() -> {
            try {
                System.out.println("任务执行完毕了");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, Runnable::run);

        // 这里可以继续执行其他操作，而不会阻塞等待异步任务完成
    }



}
