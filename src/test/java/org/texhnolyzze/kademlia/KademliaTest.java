package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KademliaTest {

    @Test
    void test() {
        KadOptions options = new KadOptions();
        options.setK(3);
        options.setSaveStateToFileIntervalMillis(-1L);
        List<Kademlia> kademlias = new ArrayList<>();
        Kademlia bootstrapNode = Kademlia.getInstance(options, null);
        InetSocketAddress address = new InetSocketAddress("localhost", bootstrapNode.getOwnerNode().getPort());
        List<InetSocketAddress> list = singletonList(address);
        System.err.println("Bootstrap node address is " + address);
        for (int i = 0; i < 50; i++) {
            Kademlia kademlia = Kademlia.getInstance(options, null);
            kademlias.add(kademlia);
            kademlia.bootstrap(list);
        }
        byte[] key = {1, 2, 3};
        byte[] val = {3, 2, 1};
        assertTrue(bootstrapNode.put(key, val));
        for (int i = 0; i < 50; i++) {
            byte[] actual = kademlias.get(i).get(key);
            assertArrayEquals(val, actual);
        }
        System.out.println(1);
    }

}