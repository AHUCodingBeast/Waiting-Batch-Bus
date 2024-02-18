package com.waiting.bus.testcase.others;

import com.waiting.bus.core.containers.ProducerBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author jianzhang
 * @date 2024/2/7
 */
public class DelayQueueDemo {
    public static void main(String[] args) throws Exception {
        DelayQueue<DelayElement> delayQueue = new DelayQueue<>();
        long nowMs = System.currentTimeMillis();
        DelayElement delayElement1 = new DelayElement(nowMs + 15000, "15000-delay-element");
        DelayElement delayElement2 = new DelayElement(nowMs + 10000, "10000-delay-element");
        DelayElement delayElement3 = new DelayElement(nowMs + 25000, "25000-delay-element");
        DelayElement delayElement4 = new DelayElement(nowMs + 5000, "5000-delay-element");
        DelayElement delayElement5 = new DelayElement(nowMs + 40000, "40000-delay-element");
        delayQueue.add(delayElement1);
        delayQueue.add(delayElement2);
        delayQueue.add(delayElement3);
        delayQueue.add(delayElement4);
        delayQueue.add(delayElement5);

        // 进程会在这里阻塞，直到有元素到时间被取走
//        DelayElement take = delayQueue.take();
//        System.out.println(take.getBizData());


//        DelayElement poll = delayQueue.poll(nowMs + 16000, TimeUnit.MILLISECONDS);
//        System.out.println(poll.getBizData());

        Thread.sleep(16000);


        List<DelayElement> list = new ArrayList<>();
        delayQueue.drainTo(list);

        for (DelayElement delayElement : list) {
            System.out.println(delayElement.getBizData());
        }

    }
}

class DelayElement implements Delayed {

    /**
     * 下次重试时间
     */
    private long nextRetryMs;

    /**
     * 业务数据
     */
    private String bizData;

    public DelayElement(long nextRetryMs, String bizData) {
        this.nextRetryMs = nextRetryMs;
        this.bizData = bizData;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(nextRetryMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return (int) (nextRetryMs - ((DelayElement) o).getNextRetryMs());
    }

    public long getNextRetryMs() {
        return nextRetryMs;
    }

    public void setNextRetryMs(long nextRetryMs) {
        this.nextRetryMs = nextRetryMs;
    }

    public String getBizData() {
        return bizData;
    }
}


