package org.texhnolyzze.kademlia;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

class KadNode {

    private final KadId id;
    private final byte[] address;
    private final Kademlia kademlia;
    private ByteString addressAsByteString;
    private final int port;
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

    private NoopClientStreamObserver<Pong> pongClientStreamObserver() {
        if (pongClientStreamObserver == null)
            pongClientStreamObserver = new NoopClientStreamObserver<>(this, kademlia);
        return pongClientStreamObserver;
    }

    KadId getId() {
        return id;
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

    void ping(Ping request) {
        stub().ping(request, pongClientStreamObserver());
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

    private KademliaGrpc.KademliaStub stub() {
        if (stub == null) {
            try {
                stub = KademliaGrpc.newStub(
                    ManagedChannelBuilder.
                    forAddress(InetAddress.getByAddress(address).getHostAddress(), port).
                    usePlaintext().
                    executor(kademlia.getExecutor()).build()
                );
            } catch (UnknownHostException e) {
                throw new KademliaException("Error getting host", e);
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
            ManagedChannel channel = (ManagedChannel) stub.getChannel();
            channel.shutdown();
        }
    }

}
