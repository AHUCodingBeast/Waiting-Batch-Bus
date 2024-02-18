package com.waiting.bus.testcase.others;

import com.google.common.collect.EvictingQueue;

/**
 * @author jianzhang
 * @date 2024/2/7
 * 主要功能是在达到预设的最大容量时自动移除最旧的元素
 */
public class EvictingQueueDemo {

    /**
     * EvictingQueue实际上是基于Java的LinkedList实现的。
     * LinkedList是一种双向链表，每个节点都有一个指向前一个节点和后一个节点的引用。EvictingQueue利用了这一点，可以通过修改节点的引用关系来实现元素的添加和移除。
     * 当向EvictingQueue中添加新元素时，首先会检查当前队列的大小是否已经达到了预设的最大容量。如果没有达到最大容量，则直接将新元素添加到链表的末尾；
     * 如果已经达到最大容量，则先从链表头部移除最旧的元素（即第一个元素），然后再将新元素添加到链表的末尾。
     * 在EvictingQueue中，元素的顺序是由它们被添加到队列中的时间决定的。因此，最新添加的元素总是位于链表的末尾，而最早添加的元素总是位于链表的头部。这样，在需要移除最旧的元素时，就可以直接从链表头部开始操作，非常高效。
     * 总之，EvictingQueue通过维护一个双向链表，并根据元素的添加时间来确定它们在链表中的位置，实现了在达到预设的最大容量时自动移除最旧元素的功能。这种实现方式既简单又高效，能够满足大多数需要限制数据存储量并自动清除过期或不再需要的数据的场景的需求。
     */

    public static void main(String[] args) {
        EvictingQueue<String> queue = EvictingQueue.create(10);
        for (int i = 1; i <= 20; i++) {
            queue.add("item" + i);
        }
        for (String item : queue) {
            System.out.println(item);
        }

    }

}
