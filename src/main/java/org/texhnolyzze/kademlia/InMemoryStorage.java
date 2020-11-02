package org.texhnolyzze.kademlia;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.singletonList;

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
    public void getAll(TriConsumer<byte[], byte[], Boolean> consumer) {
        lock.readLock().lock();
        try {
            for (Iterator<Map.Entry<ByteKey, TimestampedData>> iterator = data.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<ByteKey, TimestampedData> entry = iterator.next();
                consumer.accept(entry.getKey().key, entry.getValue().data, !iterator.hasNext());
            }
        } finally {
            lock.readLock().unlock();
        }
    }
    
    @Override
    public void getOlderThan(long durationMillis, TriConsumer<byte[], byte[], Boolean> consumer) {
        long time = System.currentTimeMillis() - durationMillis;
        lock.readLock().lock();
        try {
            SortedMap<Long, List<ByteKey>> subMap = timestampIndex.headMap(time);
            for (Iterator<Map.Entry<Long, List<ByteKey>>> iter1 = subMap.entrySet().iterator(); iter1.hasNext(); ) {
                Map.Entry<Long, List<ByteKey>> entry = iter1.next();
                for (Iterator<ByteKey> iter2 = entry.getValue().iterator(); iter2.hasNext(); ) {
                    ByteKey key = iter2.next();
                    consumer.accept(key.key, data.get(key).data, !iter1.hasNext() && !iter2.hasNext());
                }
            }
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
