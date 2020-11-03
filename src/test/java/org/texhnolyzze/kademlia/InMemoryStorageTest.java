package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStorageTest {

    @Test
    void test() throws InterruptedException {
        InMemoryStorage storage = new InMemoryStorage();
        byte[] key = {0, 1, 2};
        byte[] val = {0, 1, 2};
        storage.put(key, val);
        int counter = 0;
        for (Map.Entry<byte[], byte[]> entry : storage.getAll()) {
            counter++;
            assertSame(entry.getKey(), key);
            assertSame(entry.getValue(), val);
        }
        assertEquals(1, counter);
        Thread.sleep(5);
        counter = 0;
        Iterable<Map.Entry<byte[], byte[]>> olderThan = storage.getOlderThan(1);
        for (Map.Entry<byte[], byte[]> entry : olderThan) {
            counter++;
            assertSame(entry.getKey(), key);
            assertSame(entry.getValue(), val);
        }
        assertEquals(1, counter);
        counter = 0;
        storage.removeOlderThan(1);
        for (Map.Entry<byte[], byte[]> entry : storage.getAll()) {
            counter++;
        }
        assertEquals(0, counter);
    }

}