package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStorageTest {

    @Test
    void test() throws InterruptedException {
        InMemoryStorage storage = new InMemoryStorage();
        byte[] key = {0, 1, 2};
        byte[] val = {0, 1, 2};
        storage.put(key, val);
        int[] counter = {0};
        storage.getAll((k, v, isLast) -> {
            counter[0]++;
            assertTrue(isLast);
            assertSame(k, key);
            assertSame(v, val);
        });
        assertEquals(1, counter[0]);
        Thread.sleep(5);
        counter[0] = 0;
        storage.getOlderThan(1, (k, v, isLast) -> {
            counter[0]++;
            assertTrue(isLast);
            assertSame(k, key);
            assertSame(v, val);
        });
        assertEquals(1, counter[0]);
        counter[0] = 0;
        storage.removeOlderThan(1);
        storage.getAll((k, v, isLast) -> {
            counter[0]++;
        });
        assertEquals(0, counter[0]);
    }

}