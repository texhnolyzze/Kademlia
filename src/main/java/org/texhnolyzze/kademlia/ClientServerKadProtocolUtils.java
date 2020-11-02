package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.ByteString;
import io.grpc.Context;

final class ClientServerKadProtocolUtils {

    private ClientServerKadProtocolUtils() {
        throw new UnsupportedOperationException();
    }

    static KadNode convertToKadNode(Node n, Kademlia kademlia) {
        return new KadNode(new KadId(n.getNodeId()), kademlia, n.getAddress().toByteArray(), n.getPort());
    }

    static void baseHandle(final KadNode node, Kademlia kademlia) {
        KadRoutingTable table = kademlia.getRoutingTable();
        if (table.contains(node))
            return;
        Storage storage = kademlia.getStorage();
        byte[] dist1 = new byte[KadId.SIZE_BYTES];
        byte[] dist2 = new byte[KadId.SIZE_BYTES];
        KadNode owner = kademlia.getOwnerNode();
        storage.getAll((key, val, isLast) -> {
            KadId storeId = new KadId(key, true);
            MinMaxPriorityQueue<KadNode> neighbours = table.getNeighboursOf(storeId, null, false);
            boolean callStore = false;
            if (!neighbours.isEmpty()) {
                KadNode last = neighbours.peekLast();
                node.getId().distanceTo(storeId, dist1);
                last.getId().distanceTo(storeId, dist2);
                if (KadId.compare(dist1, dist2) < 0) {
                    KadNode first = neighbours.peekFirst();
                    owner.getId().distanceTo(storeId, dist1);
                    first.getId().distanceTo(storeId, dist2);
                    callStore = KadId.compare(dist1, dist2) < 0;
                }
            } else
                callStore = true;
            if (callStore) {
                Context.current().fork().run(() -> {
                    node.store(
                        kademlia.getStoreRequestBuilder().
                            setKey(ByteString.copyFrom(key)).
                            setVal(ByteString.copyFrom(val)).
                            build()
                    );
                });
            }
        });
        table.addNode(node);
    }
}
