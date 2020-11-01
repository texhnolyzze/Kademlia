package org.texhnolyzze.kademlia;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NoopClientStreamObserver<E> implements StreamObserver<E> {

    private static final Logger LOG = LoggerFactory.getLogger(NoopClientStreamObserver.class);

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
        try {
            if (node.getId() != null)
                LOG.error("Error when contacting node with id {} located on {}:{}", node.getId(), node.getAddressAsString(), node.getPort(), throwable);
            else
                LOG.error("Error when contacting node located on {}:{}", node.getAddressAsByteString(), node.getPort(), throwable);
        } finally {
            if (node.getId() != null)
                kademlia.getRoutingTable().removeNode(node);
        }
    }

}
