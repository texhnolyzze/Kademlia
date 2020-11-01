package org.texhnolyzze.kademlia;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Collections.singletonList;

class InMemoryStorage implements Storage {

    private final Map<byte[], TimestampedData> data = new HashMap<>();
    private final SortedMap<Long, List<byte[]>> timestampIndex = new TreeMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public byte[] get(byte[] key) {
        lock.readLock().lock();
        try {
            TimestampedData tsData = this.data.get(key);
            lock.readLock().unlock();
            return tsData.data;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(byte[] key, byte[] val) {
        long timestamp = System.currentTimeMillis();
        lock.writeLock().lock();
        try {
            TimestampedData tsData = data.get(key);
            if (tsData != null) {
//              this timestamp should always be here, but just in case we use computeIfPresent
                timestampIndex.computeIfPresent(tsData.timestamp, (ts, list) -> {
                    if (list.size() == 1)
                        return null;
                    list.remove(key);
                    return list;
                });
            }
            data.put(key, new TimestampedData(val, timestamp));
            timestampIndex.compute(timestamp, (ts, list) -> {
                if (list == null)
                    return singletonList(key);
                if (list.size() != 1) {
                    list.add(key);
                    return list;
                }
                List<byte[]> newList = new ArrayList<>(2);
                newList.add(list.get(0));
                newList.add(key);
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
            for (Iterator<Map.Entry<byte[], TimestampedData>> iterator = data.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<byte[], TimestampedData> entry = iterator.next();
                consumer.accept(entry.getKey(), entry.getValue().data, !iterator.hasNext());
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
            SortedMap<Long, List<byte[]>> subMap = timestampIndex.headMap(time);
            for (Iterator<Map.Entry<Long, List<byte[]>>> iter1 = subMap.entrySet().iterator(); iter1.hasNext(); ) {
                Map.Entry<Long, List<byte[]>> entry = iter1.next();
                for (Iterator<byte[]> iter2 = entry.getValue().iterator(); iter2.hasNext(); ) {
                    byte[] key = iter2.next();
                    consumer.accept(key, data.get(key).data, !iter1.hasNext() && !iter2.hasNext());
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
            SortedMap<Long, List<byte[]>> subMap = timestampIndex.headMap(time);
            for (Map.Entry<Long, List<byte[]>> entry : subMap.entrySet()) {
                for (byte[] key : entry.getValue()) {
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
            TimestampedData tsData = this.data.remove(key);
            if (tsData != null) {
//              this timestamp should always be here, but just in case we use computeIfPresent
                timestampIndex.computeIfPresent(tsData.timestamp, (ts, list) -> {
                    if (list.size() == 1)
                        return null;
                    list.remove(tsData.data);
                    return list;
                });
            }
        } finally {
            lock.writeLock().unlock();
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
