package org.texhnolyzze.kademlia;

import com.google.common.base.Preconditions;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.ByteString;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.*;
import static java.util.stream.Collectors.toList;

public class Kademlia {

    private static final Path STATE_FILE = Path.of("kad-state");

    static final Context.Key<InetSocketAddress> REMOTE_ADDR = Context.key("remote-addr");

    private static final Logger LOG = LoggerFactory.getLogger(Kademlia.class);

    private final Ping ping;
    private final StoreRequest storeRequest;
    private final FindNodeRequest findNodeRequest;
    private final FindValueRequest findValueRequest;

    private final Pong pong;
    private final FindValueResponse findValueResponse;
    private final FindNodeResponse findNodeResponse;

    private final Storage storage;
    private final KadOptions options;
    private final ScheduledExecutorService executor;
    private final Executor grpcExecutor;
    private final KadRoutingTable routingTable;
    private final AtomicLong version;

    private KadNode ownerNode;

    private Server server;

    private Kademlia(KadOptions options, Storage storage) {
        if (options == null)
            this.options = new KadOptions();
        else
            this.options = options.copy();
        this.storage = Objects.requireNonNullElseGet(storage, InMemoryStorage::new);
        this.executor = Executors.newScheduledThreadPool(this.options.getKademliaPoolSize());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (this.server != null) {
                LOG.info("Shutting down server");
                try {
                    this.server.shutdown();
                    LOG.info("Server shutdown");
                } catch (Exception e) {
                    LOG.error("Error shutting down server", e);
                }
            }
        }));
        int port;
        State state = loadFromFile();
        if (state != null) {
            this.version = new AtomicLong(state.version);
            port = this.options.isOverwritePersistedPort() ? this.options.getPort() : state.port;
            ownerNode = new KadNode(new KadId(state.nodeId, true), this, state.port, -1);
        } else {
            this.version = new AtomicLong();
            port = this.options.getPort();
        }
        this.routingTable = new KadRoutingTable(this);
        if (state != null) {
            for (KadNode node : state.neighbours)
                this.routingTable.addNode(node);
        }
        try {
            this.grpcExecutor = Executors.newFixedThreadPool(this.options.getGrpcPoolSize());
            server = ServerBuilder.forPort(port).addService(new KadService(this)).executor(grpcExecutor).intercept(new ServerInterceptor() {
                @Override
                public <R0, R1> ServerCall.Listener<R0> interceptCall(ServerCall<R0, R1> call, Metadata headers, ServerCallHandler<R0, R1> next) {
                    InetSocketAddress address = (InetSocketAddress) call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                    Context context = Context.current().withValue(REMOTE_ADDR, address);
                    return Contexts.interceptCall(context, call, headers, next);
                }
            }).build();
            server.start();
            if (Objects.isNull(ownerNode))
                ownerNode = new KadNode(new KadId(), this, server.getPort(), -1);
            LOG.info("Server started listening on port {}", server.getPort());
        } catch (IOException e) {
            throw new KademliaException("Can't initialize server", e);
        }
        ByteString owner = ownerNode.getId().asByteString();
        port = ownerNode.getPort();
        this.ping = Ping.newBuilder().setNodeId(owner).setPort(port).build();
        this.storeRequest = StoreRequest.newBuilder().setNodeId(owner).setPort(port).build();
        this.findNodeRequest = FindNodeRequest.newBuilder().setNodeId(owner).setPort(port).build();
        this.findValueRequest = FindValueRequest.newBuilder().setNodeId(owner).setPort(port).build();
        this.pong = Pong.newBuilder().setNodeId(owner).build();
        this.findNodeResponse = FindNodeResponse.newBuilder().setNodeId(owner).build();
        this.findValueResponse = FindValueResponse.newBuilder().setNodeId(owner).build();
    }

    private State loadFromFile() {
        if (Files.exists(STATE_FILE)) {
            try (DataInputStream stream = new DataInputStream(Files.newInputStream(STATE_FILE, READ))) {
                long version = stream.readLong();
                int numNeighbours = stream.readUnsignedShort();
                int port = stream.readUnsignedShort();
                byte[] nodeId = new byte[KadId.SIZE_BYTES];
                readByteArray(nodeId, stream);
                List<KadNode> neighbours = new ArrayList<>(numNeighbours);
                for (int i = 0; i < numNeighbours; i++) {
                    long neighbourVersion = stream.readLong();
                    byte[] neighbourId = new byte[KadId.SIZE_BYTES];
                    readByteArray(neighbourId, stream);
                    int neighbourPort = stream.readUnsignedShort();
                    byte[] neighbourAddr = new byte[4];
                    readByteArray(neighbourAddr, stream);
                    neighbours.add(new KadNode(new KadId(neighbourId, true), this, neighbourAddr, neighbourPort, neighbourVersion));
                }
                Preconditions.checkState(stream.read() == -1, "EOF expected");
                return new State(version, port, nodeId, neighbours);
            } catch (Exception e) {
                LOG.warn("Error reading from file", e);
            }
        }
        return null;
    }

    private void readByteArray(byte[] dest, InputStream stream) throws IOException {
        int n = stream.read(dest);
        if (n != dest.length)
            throw new KademliaException("Expected " + dest.length + " bytes, got " + n);
    }

    private void saveToFile() {
        try (DataOutputStream stream = new DataOutputStream(Files.newOutputStream(STATE_FILE, WRITE, TRUNCATE_EXISTING, CREATE))) {
            Collection<KadNode> neighbours = routingTable.getNeighboursOf(ownerNode.getId(), null, false);
            stream.writeLong(version.get());
            stream.writeShort(neighbours.size());
            stream.writeShort(ownerNode.getPort());
            stream.write(ownerNode.getId().getRaw());
            for (KadNode node : neighbours) {
                stream.writeLong(node.version());
                stream.write(node.getId().getRaw());
                stream.writeShort(node.getPort());
                stream.write(node.getAddress());
            }
            stream.flush();
        } catch (Exception e) {
            LOG.warn("Error saving state to file", e);
        }
    }

    KadNode getOwnerNode() {
        return ownerNode;
    }

    public byte[] get(byte[] key) {
        KadId id = new KadId(key, false);
        byte[] res = storage.get(id.getRaw());
        if (res != null)
            return res;
        MinMaxPriorityQueue<KadNode> neighbours = routingTable.getNeighboursOf(id, null, true);
        if (neighbours.isEmpty()) {
            LOG.warn("Can't get key {}, no neighbours found", id);
            return null;
        }
        ValueResolver resolver = new ValueResolver(id, this, neighbours);
        return resolver.resolve();
    }

    public boolean put(byte[] key, byte[] val) {
        return put0(new KadId(key, false), val);
    }

    private boolean put0(KadId key, byte[] val) {
        if (val == null || val.length == 0)
            throw new IllegalArgumentException("Empty/Null value not allowed.");
        MinMaxPriorityQueue<KadNode> neighbours = routingTable.getNeighboursOf(key, null, true);
        if (neighbours.isEmpty()) {
            LOG.warn("Can't publish key {}. No neighbours found.", key);
            return false;
        }
        NodeResolver resolver = new NodeResolver(key, this, neighbours);
        neighbours = resolver.resolve();
        byte[] dist1 = getOwnerNode().getId().distanceTo(key);
        byte[] dist2 = neighbours.peekLast().getId().distanceTo(key);
        boolean storeInLocal = KadId.compare(dist1, dist2) < 0;
        if (storeInLocal)
            storage.put(key.getRaw(), val);
        else
            storage.remove(key.getRaw());
        ByteString valAsByteString = ByteString.copyFrom(val);
        StoreRequest request = getStoreRequestBuilder().
            setKey(key.asByteString()).
            setVal(valAsByteString).
            build();
        final boolean[] atLeastOneStored = {false};
        CountDownLatch latch = new CountDownLatch(neighbours.size());
        for (KadNode node : neighbours) {
            node.store(request, new NoopClientStreamObserver<>(node, this) {

                @Override
                public void onCompleted() {
                    try {
                        atLeastOneStored[0] = true;
                        super.onCompleted();
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    try {
                        super.onError(throwable);
                    } finally {
                        latch.countDown();
                    }
                }

            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Store request interrupted", e);
        }
        return atLeastOneStored[0];
    }

    public void bootstrap() {
        MinMaxPriorityQueue<KadNode> neighbours = routingTable.getNeighboursOf(getOwnerNode().getId(), null, true);
        if (neighbours.isEmpty()) {
            throw new KademliaException("Can't perform bootstrap. No neighbours found.");
        }
        bootstrap(neighbours);
    }

    public void bootstrap(List<InetSocketAddress> addresses) {
        List<KadNode> nodes = addresses.stream().map(address ->
            new KadNode(address.getAddress().getAddress(), address.getPort(), this, -1)
        ).collect(toList());
        CountDownLatch latch = new CountDownLatch(nodes.size());
        byte[] dist1 = new byte[KadId.SIZE_BYTES];
        byte[] dist2 = new byte[KadId.SIZE_BYTES];
        MinMaxPriorityQueue<KadNode> neighbours = MinMaxPriorityQueue.<KadNode>orderedBy((n1, n2) -> {
            n1.getId().distanceTo(getOwnerNode().getId(), dist1);
            n2.getId().distanceTo(getOwnerNode().getId(), dist2);
            return KadId.compare(dist1, dist2);
        }).maximumSize(getOptions().getK()).create();
        for (KadNode node : nodes) {
            node.ping(new NoopClientStreamObserver<>(node, this) {

                @Override
                public void onNext(Pong value) {
                    try {
                        node.setId(new KadId(value.getNodeId()));
                        synchronized (neighbours) {
                            if (!neighbours.contains(node))
                                neighbours.add(node);
                        }
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    try {
                        super.onError(throwable);
                        synchronized (neighbours) {
                            neighbours.remove(node);
                        }
                    } finally {
                        latch.countDown();
                    }
                }

            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Bootstrap process interrupted", e);
            return;
        }
        bootstrap(neighbours);
    }

    private void bootstrap(MinMaxPriorityQueue<KadNode> neighbours) {
        NodeResolver resolver = new NodeResolver(getOwnerNode().getId(), this, neighbours);
        resolver.resolve();
    }

    private void refreshBuckets() {
        for (Iterator<KadId> iterator = routingTable.getLonelyBucketsRandomIds().iterator(); iterator.hasNext(); ) {
            KadId id = iterator.next();
            MinMaxPriorityQueue<KadNode> neighbours = routingTable.getNeighboursOf(id, null, true);
            if (neighbours.isEmpty()) {
                LOG.warn("Can't refresh bucket for key {}. No neighbours found", id);
                continue;
            }
            NodeResolver resolver = new NodeResolver(id, this, neighbours);
            if (iterator.hasNext())
                executor.execute(resolver::resolve);
            else // this method invoked from executor, so for last element we should make call directly to better utilize thread pool
                resolver.resolve();
        }
    }

    private void republishKeys() {
        Iterable<Map.Entry<byte[], byte[]>> entries = storage.getOlderThan(options.getRepublishKeyIntervalMillis());
        for (Iterator<Map.Entry<byte[], byte[]>> iterator = entries.iterator(); iterator.hasNext(); ) {
            Map.Entry<byte[], byte[]> entry = iterator.next();
            KadId id = new KadId(entry.getKey(), true);
            if (iterator.hasNext()) {
                executor.execute(() -> put0(id, entry.getValue()));
            } else
                put0(id, entry.getValue()); // this method invoked from executor, so for last element we should make call directly to better utilize thread pool
        }
    }

    /**
     * Given two nodes n1 and n2 there are 4 possibilities:
     * 1) n1.address == n2.address && n1.id == n2.id -- Same node
     * 2) n1.address != n2.address && n1.id != n2.id -- Different nodes
     * 3) n1.address == n2.address && n1.id != n2.id -- Most probably same node that failed, then returned, but it's id was not persisted
     * 4) n1.address != n2.address && n1.id == n2.id -- Same as above, but this time address was not persisted
     * So, for case 3 and 4 we must detect such nodes in our routing table and compare their versions.
     * Node with smaller version is removed and in 4th case it's ManagedChannel is also shutdown.
     */
    private void detectStaleNodes() {
        ArrayList<KadNode> nodes = routingTable.collectAll();
        for (int i = 0; i < nodes.size() - 1; i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                KadNode n1 = nodes.get(i);
                KadNode n2 = nodes.get(j);
                boolean addressEq = n1.addressEq(n2);
                boolean idEq = n1.getId().equals(n2.getId());
                if ((addressEq && !idEq) || (!addressEq && idEq)) {
                    KadNode loser = n1.version() < n2.version() ? n2 : n1;
                    routingTable.removeNode(loser);
                }
            }
        }
    }

    private void deleteStaleKeys() {
        storage.removeOlderThan(options.getKeyMaxLifetimeMillis());
    }

    public static Kademlia getInstance(KadOptions options, Storage storage) {
        Kademlia kademlia = new Kademlia(options, storage);
        KadOptions opts = kademlia.getOptions();
        ScheduledExecutorService exec = kademlia.getExecutor();
        if (opts.getSaveStateToFileIntervalMillis() > 0L) {
            exec.scheduleWithFixedDelay(
                kademlia::saveToFile,
                0L,
                opts.getSaveStateToFileIntervalMillis(),
                TimeUnit.MILLISECONDS
            );
        }
        exec.scheduleWithFixedDelay(
            kademlia::refreshBuckets,
            opts.getRefreshIntervalMillis(),
            opts.getRefreshIntervalMillis(),
            TimeUnit.MILLISECONDS
        );
        exec.scheduleWithFixedDelay(
            kademlia::republishKeys,
            opts.getRepublishKeyIntervalMillis(),
            opts.getRepublishKeyIntervalMillis(),
            TimeUnit.MILLISECONDS
        );
        if (opts.getKeyMaxLifetimeMillis() > 0L) {
            exec.scheduleWithFixedDelay(
                kademlia::deleteStaleKeys,
                opts.getKeyMaxLifetimeMillis(),
                opts.getKeyMaxLifetimeMillis(),
                TimeUnit.MILLISECONDS
            );
        }
        exec.scheduleWithFixedDelay(
            kademlia::detectStaleNodes,
            opts.getDetectStaleNodesIntervalMillis(),
            opts.getDetectStaleNodesIntervalMillis(),
            TimeUnit.MILLISECONDS
        );
        return kademlia;
    }

    ScheduledExecutorService getExecutor() {
        return executor;
    }

    Executor getGrpcExecutor() {
        return grpcExecutor;
    }

    KadOptions getOptions() {
        return options;
    }

    KadRoutingTable getRoutingTable() {
        return routingTable;
    }

    Storage getStorage() {
        return storage;
    }

    Ping getPing() {
        return ping;
    }

    StoreRequest.Builder getStoreRequestBuilder() {
        return StoreRequest.newBuilder(storeRequest);
    }

    FindNodeRequest.Builder getFindNodeRequestBuilder() {
        return FindNodeRequest.newBuilder(findNodeRequest);
    }

    FindValueRequest.Builder getFindValueRequestBuilder() {
        return FindValueRequest.newBuilder(findValueRequest);
    }

    Pong getPong() {
        return pong;
    }

    FindNodeResponse.Builder getFindNodeResponseBuilder() {
        return FindNodeResponse.newBuilder(findNodeResponse);
    }

    FindValueResponse.Builder getFindValueResponseBuilder() {
        return FindValueResponse.newBuilder(findValueResponse);
    }

    long nextVersion() {
        return version.getAndIncrement();
    }

    private static class State {

        private final long version;
        private final int port;
        private final byte[] nodeId;
        private final List<KadNode> neighbours;

        private State(long version, int port, byte[] nodeId, List<KadNode> neighbours) {
            this.version = version;
            this.port = port;
            this.nodeId = nodeId;
            this.neighbours = neighbours;
        }

    }

}
