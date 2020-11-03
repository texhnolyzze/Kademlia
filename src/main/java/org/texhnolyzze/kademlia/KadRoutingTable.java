package org.texhnolyzze.kademlia;

import com.google.common.collect.MinMaxPriorityQueue;
import io.grpc.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class KadRoutingTable {

    private static final Logger LOG = LoggerFactory.getLogger(KadRoutingTable.class);

    private final Node root;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Kademlia kademlia;

    KadRoutingTable(Kademlia kademlia) {
        this.kademlia = kademlia;
        this.root = new Node(new KBucket(KadId.MIN, KadId.MAX));
    }

    Iterable<KadId> getLonelyBucketsRandomIds() {
        lock.readLock().lock();
        try {
            List<KadId> res = new ArrayList<>();
            collectLonelyBuckets(root, res);
            return res;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void collectLonelyBuckets(Node n, List<KadId> collectTo) {
        if (n == null)
            return;
        if (n.isLeaf() && System.currentTimeMillis() - n.bucket.lastUpdated > kademlia.getOptions().getRefreshIntervalMillis())
            collectTo.add(KadId.randomBetween(n.bucket.min, n.bucket.max));
        collectLonelyBuckets(n.left, collectTo);
        collectLonelyBuckets(n.right, collectTo);
    }

    void addNode(KadNode node) {
        lock.writeLock().lock();
        try {
            Node n = locate(node);
            if (!n.bucket.addNode(node, kademlia)) {
                if (n.bucket.holds(kademlia.getOwnerNode())) {
                    n.split();
                    addNode(node);
                } else {
                    Context.current().fork().run(() ->
                        n.bucket.leastRecentlyUpdated().ping()
                    );
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    void removeNode(KadNode node) {
        lock.writeLock().lock();
        try {
            Node n = locate(node);
            if (n.bucket.removeNode(node))
                node.dispose();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean getNeighboursOf(Node node, KadId key, KadId exclude, int idx, boolean updateTimestamp, MinMaxPriorityQueue<KadNode> res) {
        if (node == null)
            return true;
        if (node.isLeaf()) {
            if (updateTimestamp)
                node.bucket.updateTimestamp();
            if (exclude == null)
                res.addAll(node.bucket.nodes);
            else {
                for (KadNode n : node.bucket.nodes) {
                    if (!n.getId().equals(exclude))
                        res.add(n);
                }
            }
        }
        if (res.size() == kademlia.getOptions().getK())
            return false;
        int bit = getBit(key.getRaw(), idx);
        boolean proceed;
        if (bit == 0) {
            proceed = getNeighboursOf(node.left, key, exclude, idx + 1, updateTimestamp, res);
            if (proceed)
                proceed = getNeighboursOf(node.right, key, exclude, idx + 1, updateTimestamp, res);
        } else {
            proceed = getNeighboursOf(node.right, key, exclude, idx + 1, updateTimestamp, res);
            if (proceed)
                proceed = getNeighboursOf(node.left, key, exclude, idx + 1, updateTimestamp, res);
        }
        return proceed;
    }

    MinMaxPriorityQueue<KadNode> getNeighboursOf(KadId key, KadId exclude, boolean updateTimestamp) {
        lock.readLock().lock();
        try {
            final byte[] dist1 = new byte[KadId.SIZE_BYTES];
            final byte[] dist2 = new byte[KadId.SIZE_BYTES];
            MinMaxPriorityQueue<KadNode> res = MinMaxPriorityQueue.<KadNode>orderedBy((node1, node2) -> {
                node1.getId().distanceTo(key, dist1);
                node2.getId().distanceTo(key, dist2);
                return KadId.compare(dist1, dist2);
            }).maximumSize(kademlia.getOptions().getK()).create();
            getNeighboursOf(root, key, exclude, 0, updateTimestamp, res);
            return res;
        } finally {
            lock.readLock().unlock();
        }
    }

    private Node locate(KadNode node) {
        Node n = root;
        byte[] raw = node.getId().getRaw();
        int bitidx = 0;
        while (!n.isLeaf()) {
            int bit = getBit(raw, bitidx++);
            if (bit == 0)
                n = n.left;
            else
                n = n.right;
        }
        return n;
    }

    private int getBit(byte[] bytes, int idx) {
        int pos = idx / 8;
        idx = idx % 8;
        byte b = bytes[pos];
        return (b >>> (7 - idx)) & 1;
    }

    boolean contains(KadNode node) {
        lock.readLock().lock();
        try {
            Node n = locate(node);
            return n.bucket.nodes.contains(node) || n.bucket.replacementCache.contains(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    private static class Node {

        private Node left;
        private Node right;
        private KBucket bucket;

        Node(KBucket bucket) {
            this.bucket = bucket;
        }

        boolean isLeaf() {
            return bucket != null;
        }

        void split() {
            Map.Entry<KBucket, KBucket> split = bucket.split();
            this.left = new Node(split.getKey());
            this.right = new Node(split.getValue());
            this.bucket = null;
        }

    }

    private static class KBucket {

        private final KadId min;
        private final KadId max;

        private long lastUpdated;

//      most recently updated nodes are at head of the list
        private final LinkedList<KadNode> nodes;
        private final LinkedList<KadNode> replacementCache;

        private KBucket(KadId min, KadId max) {
            this.min = min;
            this.max = max;
            this.nodes = new LinkedList<>();
            this.replacementCache = new LinkedList<>();
        }

        KadNode leastRecentlyUpdated() {
            return nodes.getLast();
        }

        boolean addNode(KadNode node, Kademlia kademlia) {
            if (!holds(node))
                LOG.warn("Attempt to insert node {} into bucket which can't hold it [{}, {}]", node.getId(), min, max);
            Iterator<KadNode> iterator = nodes.iterator();
            while (iterator.hasNext()) {
                KadNode next = iterator.next();
                if (next.getId().equals(node.getId())) {
                    next.transferStubOrDispose(node);
                    iterator.remove();
                    break;
                }
            }
            if (nodes.size() < kademlia.getOptions().getK()) {
                nodes.addFirst(node);
                return true;
            } else {
                iterator = replacementCache.iterator();
                while (iterator.hasNext()) {
                    KadNode next = iterator.next();
                    if (next.getId().equals(node.getId())) {
                        next.transferStubOrDispose(node);
                        iterator.remove();
                        break;
                    }
                }
                while (replacementCache.size() >= kademlia.getOptions().getReplacementCacheSize()) {
                    replacementCache.removeLast();
                }
                replacementCache.addFirst(node);
                return false;
            }
        }

        void updateTimestamp() {
            lastUpdated = System.currentTimeMillis();
        }

        Map.Entry<KBucket, KBucket> split() {
            KadId mid = KadId.mid(min, max);
            KBucket left = new KBucket(min, mid);
            KBucket right = new KBucket(mid.addOne(), max);
            splitNodes(mid, nodes, left.nodes, right.nodes);
            splitNodes(mid, replacementCache, left.replacementCache, right.replacementCache);
            return Map.entry(left, right);
        }

        private void splitNodes(KadId mid, List<KadNode> nodes, List<KadNode> left, List<KadNode> right) {
            for (KadNode node : nodes) {
                if (node.getId().compareTo(mid) < 0)
                    left.add(node);
                else
                    right.add(node);
            }
        }

        boolean holds(KadNode node) {
            return min.compareTo(node.getId()) <= 0 && node.getId().compareTo(max) <= 0;
        }

        boolean removeNode(KadNode node) {
            Iterator<KadNode> iter = nodes.iterator();
            while (iter.hasNext()) {
                KadNode next = iter.next();
                if (next.getId().equals(node.getId())) {
                    iter.remove();
                    if (!replacementCache.isEmpty())
                        nodes.addFirst(replacementCache.removeFirst());
                    return true;
                }
            }
            iter = replacementCache.iterator();
            while (iter.hasNext()) {
                KadNode next = iter.next();
                if (next.getId().equals(node.getId())) {
                    iter.remove();
                    return true;
                }
            }
            return false;
        }

    }

}
