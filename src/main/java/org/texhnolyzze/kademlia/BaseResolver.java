package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.stream.Collectors.toSet;

abstract class BaseResolver<RPC_REQUEST, RPC_RESPONSE, RESULT> {

    private static final Logger LOG = LoggerFactory.getLogger(BaseResolver.class);

    final KadId key;
    final Kademlia kademlia;
    final MinMaxPriorityQueue<KadNode> neighbours;
    private final Set<KadId> contacted;
    private final ReentrantLock lock = new ReentrantLock();
    private final Phaser phaser = new Phaser(1);
    private final Set<KadId> lastNeighbours;

    BaseResolver(KadId key, Kademlia kademlia, MinMaxPriorityQueue<KadNode> neighbours) {
        this.key = key;
        this.kademlia = kademlia;
        this.neighbours = neighbours;
        this.contacted = this.neighbours.stream().map(KadNode::getId).collect(toSet());
        this.lastNeighbours = new HashSet<>(this.contacted);
    }

    RESULT resolve() {
        ByteString ownerId = kademlia.getOwnerNode().getId().asByteString();
        RPC_REQUEST request = getRequest(ownerId, key.asByteString());
        lock.lock();
        try {
            for (KadNode node : neighbours) {
                phaser.register();
                callMethod(node, request, createObserver(node, request));
            }
        } finally {
            lock.unlock();
        }
        phaser.arriveAndAwaitAdvance();
        return getResult();
    }

    abstract RESULT getResult();
    abstract RPC_REQUEST getRequest(ByteString ownerId, ByteString key);
    abstract void callMethod(KadNode node, RPC_REQUEST request, BaseResolverStreamObserver<RPC_RESPONSE> observer);
    abstract BaseResolverStreamObserver<RPC_RESPONSE> createObserver(KadNode node, RPC_REQUEST request);

    abstract class BaseResolverStreamObserver<RPC_RESPONSE> extends NoopClientStreamObserver<RPC_RESPONSE> {

        private final RPC_REQUEST request;

        BaseResolverStreamObserver(KadNode node, RPC_REQUEST request) {
            super(node, BaseResolver.this.kademlia);
            this.request = request;
        }

        abstract void process(RPC_RESPONSE response);
        abstract boolean proceedAfter(RPC_RESPONSE response);
        abstract List<Node> extractNodeList(RPC_RESPONSE response);

        @Override
        public void onNext(RPC_RESPONSE response) {
            try {
                lock.lock();
                try {
                    process(response);
                    if (!proceedAfter(response))
                        return;
                    for (Node n : extractNodeList(response)) {
                        KadNode kadNode = ClientServerKadProtocolUtils.convertToKadNode(n, kademlia);
                        if (!neighbours.contains(kadNode))
                            neighbours.add(kadNode);
                    }
                    boolean sameAsInPreviousQuery = true;
                    for (KadNode n : neighbours) {
                        if (!lastNeighbours.contains(n.getId())) {
                            sameAsInPreviousQuery = false;
                            break;
                        }
                    }
                    if (!sameAsInPreviousQuery) {
                        lastNeighbours.clear();
                        for (KadNode n : neighbours) {
                            lastNeighbours.add(n.getId());
                        }
                    }
                    int count = sameAsInPreviousQuery ? neighbours.size() : kademlia.getOptions().getAlpha();
                    for (KadNode nextNode : neighbours) {
                        if (!contacted.contains(nextNode.getId())) {
                            count--;
                            Context.current().fork().run(() -> {
                                phaser.register();
                                callMethod(nextNode, request, createObserver(nextNode, request));
                            });
                            contacted.add(nextNode.getId());
                            if (count == 0)
                                break;
                        }
                    }
                } finally {
                    lock.unlock();
                }
            } finally {
                phaser.arriveAndDeregister();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            try {
                lock.lock();
                try {
                    neighbours.remove(node);
                    super.onError(throwable);
                } finally {
                    lock.unlock();
                }
            } finally {
                phaser.arriveAndDeregister();
            }
        }

    }

}
