package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MinMaxPriorityQueueTest {

    @Test
    void test() {
        int n = 100;
        MinMaxPriorityQueue<Integer> queue = MinMaxPriorityQueue.maximumSize(n).create();
        for (int i = 0; i < 1000; i++) {
            queue.add(i);
        }
        assertEquals(n, queue.size());
        boolean[] match = new boolean[n];
        for (Integer i : queue) {
            assertTrue(i >= 0 && i < n);
            match[i] = true;
        }
        for (boolean b : match) {
            assertTrue(b);
        }
    }

}
