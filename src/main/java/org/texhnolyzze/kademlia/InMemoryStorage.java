package org.texhnolyzze.kademlia;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

class InMemoryStorage implements Storage {

    private final Map<ByteKey, TimestampedData> data = new HashMap<>();
    private final SortedMap<Long, List<ByteKey>> timestampIndex = new TreeMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public byte[] get(byte[] key) {
        lock.readLock().lock();
        try {
            TimestampedData tsData = this.data.get(new ByteKey(key));
            return tsData == null ? null : tsData.data;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] val) {
        long timestamp = System.currentTimeMillis();
        lock.writeLock().lock();
        try {
            ByteKey bk = new ByteKey(key);
            TimestampedData tsData = data.get(bk);
            if (tsData != null) {
//              this timestamp should always be here, but just in case we use computeIfPresent
                timestampIndex.computeIfPresent(tsData.timestamp, (ts, list) -> {
                    if (list.size() == 1)
                        return null;
                    list.remove(bk);
                    return list;
                });
            }
            data.put(bk, new TimestampedData(val, timestamp));
            timestampIndex.compute(timestamp, (ts, list) -> {
                if (list == null)
                    return singletonList(bk);
                if (list.size() != 1) {
                    list.add(bk);
                    return list;
                }
                List<ByteKey> newList = new ArrayList<>(2);
                newList.add(list.get(0));
                newList.add(bk);
                return newList;
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Iterable<Map.Entry<byte[], byte[]>> getAll() {
        lock.readLock().lock();
        try {
            return data.entrySet().stream().map(entry -> Map.entry(entry.getKey().key, entry.getValue().data)).collect(toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public Iterable<Map.Entry<byte[], byte[]>> getOlderThan(long durationMillis) {
        long time = System.currentTimeMillis() - durationMillis;
        lock.readLock().lock();
        try {
            SortedMap<Long, List<ByteKey>> subMap = timestampIndex.headMap(time);
            return subMap.values().stream().flatMap(Collection::stream).map(byteKey ->
                Map.entry(byteKey.key, data.get(byteKey).data)).collect(toList()
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void removeOlderThan(long durationMillis) {
        long time = System.currentTimeMillis() - durationMillis;
        lock.writeLock().lock();
        try {
            SortedMap<Long, List<ByteKey>> subMap = timestampIndex.headMap(time);
            for (Map.Entry<Long, List<ByteKey>> entry : subMap.entrySet()) {
                for (ByteKey key : entry.getValue()) {
                    data.remove(key);
                }
            }
            subMap.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void remove(byte[] key) {
        lock.writeLock().lock();
        try {
            ByteKey bk = new ByteKey(key);
            TimestampedData tsData = this.data.remove(bk);
            if (tsData != null) {
//              this timestamp should always be here, but just in case we use computeIfPresent
                timestampIndex.computeIfPresent(tsData.timestamp, (ts, list) -> {
                    if (list.size() == 1)
                        return null;
                    list.remove(bk);
                    return list;
                });
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class ByteKey {

        private final byte[] key;

        ByteKey(byte[] key) {
            this.key = key;
        }

        @Override
        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        public boolean equals(Object o) {
            ByteKey byteKey = (ByteKey) o;
            return Arrays.equals(key, byteKey.key);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

    }

    private static class TimestampedData {

        final byte[] data;
        final long timestamp;

        private TimestampedData(byte[] data, long timestamp) {
            this.data = data;
            this.timestamp = timestamp;
        }

    }

}
