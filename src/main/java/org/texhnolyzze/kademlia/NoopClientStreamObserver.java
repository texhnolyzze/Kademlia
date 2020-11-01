package org.texhnolyzze.kademlia;

import io.grpc.stub.StreamObserver;

class NoopClientStreamObserver<E> implements StreamObserver<E> {

    final KadNode node;
    final Kademlia kademlia;

    NoopClientStreamObserver(KadNode node, Kademlia kademlia) {
        this.node = node;
        this.kademlia = kademlia;
    }

    @Override
    public void onCompleted() {
        ClientServerKadProtocolUtils.baseHandle(node, kademlia);
    }

    @Override
    public void onNext(E value) {
//      empty here
    }

    @Override
    public void onError(Throwable throwable) {
        kademlia.getRoutingTable().removeNode(node);
    }

}
