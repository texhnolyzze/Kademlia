package org.texhnolyzze.kademlia;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

class KadNode {

    private static final Logger LOG = LoggerFactory.getLogger(KadNode.class);

    private KadId id;
    private final byte[] address;
    private final Kademlia kademlia;
    private ByteString addressAsByteString;
    private String addressAsString;
    private int port;
    private KademliaGrpc.KademliaStub stub;
    private NoopClientStreamObserver<Pong> pongClientStreamObserver;

    KadNode(KadId id, Kademlia kademlia) {
        this(id, kademlia, null, -1);
    }

    KadNode(KadId id, Kademlia kademlia, byte[] address, int port) {
        this.id = id;
        this.kademlia = kademlia;
        this.address = address;
        this.port = port;
    }

    KadNode(byte[] address, int port, Kademlia kademlia) {
        this(null, kademlia, address, port);
    }

    void setId(KadId id) {
        this.id = id;
    }

    private NoopClientStreamObserver<Pong> pongClientStreamObserver() {
        if (pongClientStreamObserver == null)
            pongClientStreamObserver = new NoopClientStreamObserver<>(this, kademlia);
        return pongClientStreamObserver;
    }

    KadId getId() {
        return id;
    }

    String getAddressAsString() {
        if (addressAsString == null) {
            try {
                return Inet4Address.getByAddress(address).getHostAddress();
            } catch (UnknownHostException e) {
                throw new KademliaException("Error converting to string", e);
            }
        }
        return addressAsString;
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

    void setPort(int port) {
        this.port = port;
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

    private static final ConcurrentMap<Map.Entry<String, Integer>, ManagedChannelHolder> CHANNELS = new ConcurrentHashMap<>();

    private KademliaGrpc.KademliaStub stub() {
        if (stub == null) {
            Map.Entry<String, Integer> entry = Map.entry(getAddressAsString(), port);
            Supplier<ManagedChannel> managedChannelSupplier = () ->
                ManagedChannelBuilder.
                forAddress(entry.getKey(), entry.getValue()).
                executor(kademlia.getGrpcExecutor()).
                usePlaintext().
                build();
            ManagedChannelHolder holder = CHANNELS.computeIfAbsent(entry, addr ->
                new ManagedChannelHolder(
                    managedChannelSupplier.get()
                )
            );
            holder.lock.lock();
            try {
                holder.refCount++;
                stub = KademliaGrpc.newStub(holder.channel);
//              previous value was null, this is possible. we should initialize channel again
                if (CHANNELS.putIfAbsent(entry, holder) == null) {
                    holder.channel = managedChannelSupplier.get();
                }
            } finally {
                holder.lock.unlock();
            }
        }
        return stub;
    }

    void transferStubOrDispose(KadNode transferTo) {
        if (stub == null)
            return;
        if (this.port == transferTo.port && Arrays.equals(this.address, transferTo.address)) {
            transferTo.stub = this.stub;
        } else {
            dispose();
        }
    }

    void dispose() {
        if (stub != null) {
            Map.Entry<String, Integer> entry = Map.entry(getAddressAsString(), port);
            ManagedChannelHolder holder = CHANNELS.get(entry);
            holder.lock.lock();
            try {
                if (--holder.refCount == 0) {
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
                        CHANNELS.remove(entry);
                    }
                }
            } finally {
                holder.lock.unlock();
            }
        }
    }

    private static class ManagedChannelHolder {

        private ManagedChannel channel;
        private final Lock lock;
        private int refCount = 1;

        private ManagedChannelHolder(ManagedChannel channel) {
            this.channel = channel;
            this.lock = new ReentrantLock();
        }

    }

}
