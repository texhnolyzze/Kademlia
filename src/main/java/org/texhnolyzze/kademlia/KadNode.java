package org.texhnolyzze.kademlia;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class KadNode {

    private static final Logger LOG = LoggerFactory.getLogger(KadNode.class);

    private KadId id;
    private final byte[] address;
    private final Kademlia kademlia;
    private ByteString addressAsByteString;
    private final String addressAsString;
    private final int port;
    private final long version;
    private KademliaGrpc.KademliaStub stub;
    private NoopClientStreamObserver<Pong> pongClientStreamObserver;

    KadNode(KadId id, Kademlia kademlia, int port, long version) {
        this(id, kademlia, null, port, version);
    }

    KadNode(KadId id, Kademlia kademlia, byte[] address, int port, long version) {
        this.id = id;
        this.kademlia = kademlia;
        this.address = address;
        this.port = port;
        this.addressAsString = getAddressAsString0();
        this.version = version < 0 ? kademlia.nextVersion() : version;
    }

    KadNode(byte[] address, int port, Kademlia kademlia, long version) {
        this(null, kademlia, address, port, version);
    }

    long version() {
        return version;
    }

    void setId(KadId id) {
        this.id = id;
    }

    private NoopClientStreamObserver<Pong> pongClientStreamObserver() {
        if (pongClientStreamObserver == null)
            pongClientStreamObserver = new NoopClientStreamObserver<>(this, kademlia);
        return pongClientStreamObserver;
    }

    boolean addressEq(KadNode other) {
        return Arrays.equals(address, other.address) && port == other.port;
    }

    KadId getId() {
        return id;
    }

    String getAddressAsString() {
        return addressAsString;
    }

    private String getAddressAsString0() {
        if (address != null) {
            try {
                return InetAddress.getByAddress(address).getHostAddress();
            } catch (UnknownHostException e) {
                throw new KademliaException("Error converting to string", e);
            }
        } else {
            return InetAddress.getLoopbackAddress().getHostAddress();
        }
    }

    byte[] getAddress() {
        return address;
    }

    ByteString getAddressAsByteString() {
        if (addressAsByteString == null)
            addressAsByteString = ByteString.copyFrom(address);
        return addressAsByteString;
    }

    int getPort() {
        return port;
    }

    void ping() {
        ping(null);
    }

    void ping(StreamObserver<Pong> observer) {
        if (observer == null)
            observer = pongClientStreamObserver();
        stub().ping(kademlia.getPing(), observer);
    }

    void store(StoreRequest request) {
        stub().store(request, pongClientStreamObserver());
    }

    void store(StoreRequest request, StreamObserver<Pong> observer) {
        stub().store(request, observer);
    }

    void findNode(FindNodeRequest request, StreamObserver<FindNodeResponse> responseObserver) {
        stub().findNode(request, responseObserver);
    }

    void findValue(FindValueRequest request, StreamObserver<FindValueResponse> responseObserver) {
        stub().findValue(request, responseObserver);
    }

    static final ConcurrentMap<Map.Entry<String, Integer>, ManagedChannelHolder> CHANNELS = new ConcurrentHashMap<>();

    synchronized KademliaGrpc.KademliaStub stub() {
        if (stub == null) {
            Map.Entry<String, Integer> entry = Map.entry(getAddressAsString(), port);
            ManagedChannelHolder h = CHANNELS.compute(entry, (k, holder) -> {
                if (holder == null) {
                    holder = new ManagedChannelHolder(
                        ManagedChannelBuilder.
                            forAddress(entry.getKey(), entry.getValue()).
                            executor(kademlia.getGrpcExecutor()).
                            usePlaintext().
                            build()
                    );
                } else
                    holder.refCount++;
                Preconditions.checkState(!holder.channel.isTerminated() && !holder.channel.isShutdown(), "Channel shutdown or terminated");
                return holder;
            });
            this.stub = KademliaGrpc.newStub(h.channel);
        }
        return stub;
    }

    synchronized void dispose() {
        if (stub != null) {
            stub = null;
            Map.Entry<String, Integer> entry = Map.entry(addressAsString, port);
            CHANNELS.computeIfPresent(entry, (k, holder) -> {
                holder.refCount--;
                if (holder.refCount == 0) {
                    Preconditions.checkState(!holder.channel.isShutdown() && !holder.channel.isTerminated(), "Channel already shutdown or terminated!");
                    ManagedChannel channel = holder.channel;
                    channel.shutdown();
                    boolean interrupted = false;
                    try {
                        while (true) {
                            try {
                                boolean terminated = channel.awaitTermination(1, TimeUnit.MINUTES);
                                if (terminated)
                                    break;
                            } catch (InterruptedException e) {
                                interrupted = true;
                                LOG.error("Channel shutdown awaiting interrupted", e);
                            }
                        }
                    } finally {
                        if (interrupted)
                            Thread.currentThread().interrupt();
                    }
                    return null;
                }
                return holder;
            });
        }
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object o) {
        if (this == o) return true;
        KadNode kadNode = (KadNode) o;
        return Objects.equals(id, kadNode.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    static class ManagedChannelHolder {

        ManagedChannel channel;
        final Lock lock;
        int refCount = 1;

        private ManagedChannelHolder(ManagedChannel channel) {
            this.channel = channel;
            this.lock = new ReentrantLock();
        }

    }

}
