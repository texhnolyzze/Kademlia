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
import static org.junit.jupiter.api.Assertions.assertTrue;

class KademliaTest {

    @Test
    void test() throws InterruptedException {
        KadOptions options = new KadOptions();
        options.setK(5);
        options.setSaveStateToFileIntervalMillis(-1L);
        options.setReplacementCacheSizeFactor(2);
        options.setDetectStaleNodesIntervalMillis(100L);
        options.setRefreshIntervalMillis(1000L);
        options.setRepublishKeyIntervalMillis(1000L);
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
        bootstrapNode.put(data.get(0).getKey(), data.get(0).getValue());
        int numGet = 0;
        AtomicInteger numMatched = new AtomicInteger();
        int numStores = 0;
        AtomicInteger numStored = new AtomicInteger();
        int n = 1000;
        for (int i = 0; i < n; i++) {
            Kademlia kademlia = kademlias.get(rand.nextInt(kademlias.size()));
            if (rand.nextDouble() < 0.5) {
                numGet++;
                Map.Entry<byte[], byte[]> entry = data.get(rand.nextInt(data.size()));
                ForkJoinPool.commonPool().execute(() -> {
                    byte[] key = entry.getKey();
                    byte[] actual = kademlia.get(key);
                    if (Arrays.equals(entry.getValue(), actual)) {
                        numMatched.getAndIncrement();
                    } else {
                        try {
                            Thread.sleep(100L); // For some reason this is necessary sometimes
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        actual = kademlia.get(key);
                        if (Arrays.equals(entry.getValue(), actual))
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
        double matchRatio = ((double) numMatched.get() / numGet) * 100;
        double storeRatio = ((double) numStored.get() / numStores) * 100;
        System.out.println("Matched " + matchRatio);
        System.out.println("Stored " + storeRatio);
        assertTrue(matchRatio >= 99);
        assertTrue(storeRatio >= 99);
    }

}