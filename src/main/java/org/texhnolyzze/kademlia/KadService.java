package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;

class KadService extends KademliaGrpc.KademliaImplBase {

    private final Kademlia kademlia;

    KadService(Kademlia kademlia) {
        this.kademlia = kademlia;
    }

    @Override
    public void ping(Ping request, StreamObserver<Pong> responseObserver) {
        KadNode node = getNode(request.getNodeId(), request.getPort());
        ClientServerKadProtocolUtils.baseHandle(node, kademlia);
        responseObserver.onNext(pong());
        responseObserver.onCompleted();
    }

    @Override
    public void store(StoreRequest request, StreamObserver<Pong> responseObserver) {
        KadNode node = getNode(request.getNodeId(), request.getPort());
        ClientServerKadProtocolUtils.baseHandle(node, kademlia);
        Storage storage = kademlia.getStorage();
        storage.put(request.getKey().toByteArray(), request.getVal().toByteArray());
        responseObserver.onNext(pong());
        responseObserver.onCompleted();
    }

    @Override
    public void findNode(FindNodeRequest request, StreamObserver<FindNodeResponse> responseObserver) {
        KadNode node = getNode(request.getNodeId(), request.getPort());
        ClientServerKadProtocolUtils.baseHandle(node, kademlia);
        KadId id = new KadId(request.getKey());
        FindNodeResponse.Builder builder = FindNodeResponse.newBuilder().setNodeId(kademlia.getOwnerNode().getId().asByteString());
        setNodes(builder, id, node.getId(), FindNodeResponse.Builder::addNodes);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void findValue(FindValueRequest request, StreamObserver<FindValueResponse> responseObserver) {
        KadNode node = getNode(request.getNodeId(), request.getPort());
        ClientServerKadProtocolUtils.baseHandle(node, kademlia);
        KadId id = new KadId(request.getKey());
        byte[] key = id.getRaw();
        Storage storage = kademlia.getStorage();
        FindValueResponse.Builder builder = FindValueResponse.newBuilder().setNodeId(kademlia.getOwnerNode().getId().asByteString());
        byte[] val = storage.get(key);
        if (val != null) {
            builder.setVal(ByteString.copyFrom(val));
        } else {
            setNodes(builder, id, node.getId(), FindValueResponse.Builder::addNodes);
        }
        responseObserver.onNext(builder.build());
    }

    private <B> void setNodes(B builder, KadId id, KadId exclude, BiConsumer<B, Node> addNode) {
        KadRoutingTable table = kademlia.getRoutingTable();
        MinMaxPriorityQueue<KadNode> neighbours = table.getNeighboursOf(id, exclude, false);
        for (KadNode n : neighbours) {
            addNode.accept(
                builder,
                Node.newBuilder().
                    setAddress(n.getAddressAsByteString()).
                    setPort(n.getPort()).
                    setNodeId(n.getId().asByteString()).
                    build()
            );
        }
    }

    private Pong pong() {
        return Pong.newBuilder().setNodeId(
            kademlia.getOwnerNode().getId().asByteString()
        ).build();
    }

    private KadNode getNode(ByteString nodeId, int port) {
        InetSocketAddress address = Kademlia.REMOTE_ADDR.get();
        return new KadNode(new KadId(nodeId), kademlia, address.getAddress().getAddress(), port);
    }

}
