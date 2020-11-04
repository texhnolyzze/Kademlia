package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KadNodeTest {

    @Test
    void testChannelsNotDuplicated() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        Kademlia kademlia = Kademlia.getInstance(null, null);
        int port = kademlia.getOwnerNode().getPort();
        byte[] address = kademlia.getOwnerNode().getAddress();
        int numDuplicateNodes = 5;
        List<KadNode> nodes = new ArrayList<>(numDuplicateNodes);
        for (int i = 0; i < numDuplicateNodes; i++)
            nodes.add(new KadNode(address, port, kademlia, 0));
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (double stubInvokeProbability = 0.0; stubInvokeProbability <= 1.0; stubInvokeProbability += 0.001) {
            for (int i = 0; i < 1000; i++) {
                int j = rand.nextInt(numDuplicateNodes);
                KadNode node = nodes.get(j);
                if (rand.nextDouble() < stubInvokeProbability) {
                    ForkJoinPool.commonPool().execute(node::stub);
                } else {
                    ForkJoinPool.commonPool().execute(node::dispose);
                }
            }
            Field stub = KadNode.class.getDeclaredField("stub");
            stub.setAccessible(true);
            ForkJoinPool.commonPool().awaitTermination(100, TimeUnit.SECONDS);
            Field channelsField = KadNode.class.getDeclaredField("CHANNELS");
            channelsField.setAccessible(true);
            ConcurrentMap<Map.Entry<String, Integer>, KadNode.ManagedChannelHolder> channels = KadNode.CHANNELS;
            int actualAllocated = 0;
            for (KadNode node : nodes) {
                if (stub.get(node) != null) {
                    actualAllocated++;
                }
            }
            if (!channels.isEmpty()) {
                assertEquals(1, channels.size());
                KadNode.ManagedChannelHolder holder = channels.values().iterator().next();
                assertTrue(holder.refCount > 0, "failed for stubInvokeProbability: " + stubInvokeProbability);
                assertEquals(holder.refCount, actualAllocated, "failed for stubInvokeProbability: " + stubInvokeProbability);
            } else {
                assertEquals(0, actualAllocated, "failed for stubInvokeProbability: " + stubInvokeProbability);
            }
        }
    }

}