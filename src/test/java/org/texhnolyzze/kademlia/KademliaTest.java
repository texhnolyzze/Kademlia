package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;

class KademliaTest {

    @Test
    void test() throws InterruptedException {
        KadOptions options = new KadOptions();
        options.setK(20);
        options.setSaveStateToFileIntervalMillis(-1L);
//        options.setRefreshIntervalMillis(50L);
//        options.setRepublishKeyIntervalMillis(50L);
        options.setGrpcPoolSize(3);
        options.setKademliaPoolSize(3);
        List<Kademlia> kademlias = new ArrayList<>();
        Kademlia bootstrapNode = Kademlia.getInstance(options, null);
        InetSocketAddress address = new InetSocketAddress("localhost", bootstrapNode.getOwnerNode().getPort());
        List<InetSocketAddress> list = singletonList(address);
        System.err.println("Bootstrap node address is " + address);
        for (int i = 0; i < 100; i++) {
            Kademlia kademlia = Kademlia.getInstance(options, null);
            kademlias.add(kademlia);
            kademlia.bootstrap(list);
        }
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        List<Map.Entry<byte[], byte[]>> data = new ArrayList<>();
        data.add(Map.entry(new byte[] {4}, new byte[] {2}));
        int numGet = 0;
        AtomicInteger numMatched = new AtomicInteger();
        int numStores = 0;
        AtomicInteger numStored = new AtomicInteger();
        int n = 10000;
        for (int i = 0; i < n; i++) {
            Kademlia kademlia = kademlias.get(rand.nextInt(kademlias.size()));
            if (rand.nextDouble() < 0.85) {
                numGet++;
                Map.Entry<byte[], byte[]> entry = data.get(rand.nextInt(data.size()));
                ForkJoinPool.commonPool().execute(() -> {
                    byte[] actual = kademlia.get(entry.getKey());
                    if (Arrays.equals(entry.getValue(), actual)) {
                        numMatched.getAndIncrement();
                    }
                });
            } else {
                numStores++;
                byte[] key = new byte[rand.nextInt(10, 15)];
                byte[] val = new byte[rand.nextInt(10, 15)];
                rand.nextBytes(key);
                rand.nextBytes(val);
                data.add(Map.entry(key, val));
                ForkJoinPool.commonPool().execute(() -> {
                    boolean stored = kademlia.put(key, val);
                    if (stored) {
                        numStored.getAndIncrement();
                    }
                });
            }
        }
        ForkJoinPool.commonPool().awaitTermination(100L, TimeUnit.SECONDS);
        System.out.println("Matched " + ((double) numMatched.get() / numGet) * 100);
        System.out.println("Stored " + ((double) numStored.get() / numStores) * 100);
    }

}