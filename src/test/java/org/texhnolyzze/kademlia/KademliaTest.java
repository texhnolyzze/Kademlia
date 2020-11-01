package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

class KademliaTest {

    @Test
    void test() {
        KadOptions options = new KadOptions();
        options.setSaveStateToFileIntervalMillis(-1L);
        List<Kademlia> kademlias = new ArrayList<>();
        Kademlia bootstrapNode = Kademlia.getInstance(options, null);
        InetSocketAddress address = new InetSocketAddress("localhost", bootstrapNode.getOwnerNode().getPort());
        List<InetSocketAddress> list = singletonList(address);
        for (int i = 0; i < 50; i++) {
            Kademlia kademlia = Kademlia.getInstance(options, null);
            kademlias.add(kademlia);
            kademlia.bootstrap(list);
        }
        System.out.println(1);
    }

}