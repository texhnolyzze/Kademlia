package org.texhnolyzze.kademlia;

import java.util.Map;

public interface Storage {

    /**
     * @param key digested key
     * @return value stored or null if none
     */
    byte[] get(byte[] key);

    /**
     * Store value
     * @param key digested key
     * @param val value
     */
    void put(byte[] key, byte[] val);

    /**
     * Get all key-value pairs
     * @return  all pairs (digested key, value)
     */
    Iterable<Map.Entry<byte[], byte[]>> getAll();

    /**
     * @return pairs (digested key, value, isLast) older than durationMillis
     */
    Iterable<Map.Entry<byte[], byte[]>> getOlderThan(long durationMillis);

    /**
     * Remove all key-value pairs older than durationMillis
     */
    void removeOlderThan(long durationMillis);

    /**
     * Remove key from storage
     * @param key digested key
     */
    void remove(byte[] key);

}
