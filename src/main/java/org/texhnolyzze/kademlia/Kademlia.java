package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.protobuf.ByteString;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

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

    private final Storage storage;
    private final KadOptions options;
    private final ScheduledExecutorService executor;
    private final Executor grpcExecutor;
    private final KadRoutingTable routingTable;

    private final KadNode ownerNode;

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
            ownerNode = new KadNode(new KadId(state.nodeId, true), this);
            port = this.options.isOverwritePersistedPort() ? this.options.getPort() : state.port;
        } else {
            port = this.options.getPort();
            ownerNode = new KadNode(new KadId(), this);
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
            ownerNode.setPort(server.getPort());
            LOG.info("Server started listening on port {}", server.getPort());
        } catch (IOException e) {
            throw new KademliaException("Can't initialize server", e);
        }
        this.ping = Ping.newBuilder().setNodeId(getOwnerNode().getId().asByteString()).setPort(getOwnerNode().getPort()).build();
        this.storeRequest = StoreRequest.newBuilder().setNodeId(getOwnerNode().getId().asByteString()).setPort(getOwnerNode().getPort()).build();
        this.findNodeRequest = FindNodeRequest.newBuilder().setNodeId(getOwnerNode().getId().asByteString()).setPort(getOwnerNode().getPort()).build();
        this.findValueRequest = FindValueRequest.newBuilder().setNodeId(getOwnerNode().getId().asByteString()).setPort(getOwnerNode().getPort()).build();
    }

    private State loadFromFile() {
        if (Files.exists(STATE_FILE)) {
            try (InputStream stream = Files.newInputStream(STATE_FILE, READ)){
                byte[] state = stream.readAllBytes();
                int port = ((state[0] & 0xFF) << 8) | (state[1] & 0xFF);
                byte[] nodeId = new byte[KadId.SIZE_BYTES];
                System.arraycopy(state, 2, nodeId, 0, KadId.SIZE_BYTES);
                List<KadNode> neighbours = new ArrayList<>();
                for (int i = 2 + KadId.SIZE_BYTES; i < state.length;) {
                    byte[] neighbourId = new byte[KadId.SIZE_BYTES];
                    System.arraycopy(state, i, neighbourId, 0, KadId.SIZE_BYTES);
                    i += KadId.SIZE_BYTES;
                    int neighbourPort = ((state[i++] & 0xFF) << 8) | (state[i++] & 0xFF);
                    byte[] neighbourAddr = new byte[4];
                    neighbourAddr[0] = state[i++];
                    neighbourAddr[1] = state[i++];
                    neighbourAddr[2] = state[i++];
                    neighbourAddr[3] = state[i++];
                    neighbours.add(new KadNode(new KadId(neighbourId, true), this, neighbourAddr, neighbourPort));
                }
                return new State(port, nodeId, neighbours);
            } catch (Exception e) {
                LOG.warn("Error reading from file", e);
            }
        }
        return null;
    }

    private void saveToFile() {
        try (OutputStream stream = Files.newOutputStream(STATE_FILE, WRITE, TRUNCATE_EXISTING, CREATE)) {
            Collection<KadNode> neighbours = routingTable.getNeighboursOf(ownerNode.getId(), null, false);
            byte[] state = new byte[(2 + KadId.SIZE_BYTES) + (neighbours.size() * (KadId.SIZE_BYTES + 2 + 4))];
            int port = ownerNode.getPort();
            int i = 0;
            state[i++] = (byte) ((port >>> 8) & 0xFF);
            state[i++] = (byte) (port & 0xFF);
            for (int j = 0; j < KadId.SIZE_BYTES; i++, j++) {
                state[i] = ownerNode.getId().getRaw()[j];
            }
            for (KadNode node : neighbours) {
                for (int j = 0; j < KadId.SIZE_BYTES; j++, i++) {
                    state[i] = node.getId().getRaw()[j];
                }
                port = node.getPort();
                state[i++] = (byte) ((port >>> 8) & 0xFF);
                state[i++] = (byte) (port & 0xFF);
                byte[] address = node.getAddress();
                state[i++] = address[0];
                state[i++] = address[1];
                state[i++] = address[2];
                state[i++] = address[3];
            }
            stream.write(state);
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
            new KadNode(address.getAddress().getAddress(), address.getPort(), this)
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

    private static class State {

        private final int port;
        private final byte[] nodeId;
        private final List<KadNode> neighbours;

        private State(int port, byte[] nodeId, List<KadNode> neighbours) {
            this.port = port;
            this.nodeId = nodeId;
            this.neighbours = neighbours;
        }

    }

}
