package org.texhnolyzze.kademlia;

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
     * @param consumer accepts triple (digested key, value, isLast)
     *                 where isLast is indicator whether this triple is last triple
     */
    void getAll(TriConsumer<byte[], byte[], Boolean> consumer);

    /**
     * Get all key-value pairs older than durationMillis
     * @param consumer accepts triple (digested key, value, isLast)
     *                 where isLast is indicator whether this triple is last triple
     */
    void getOlderThan(long durationMillis, TriConsumer<byte[], byte[], Boolean> consumer);

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
