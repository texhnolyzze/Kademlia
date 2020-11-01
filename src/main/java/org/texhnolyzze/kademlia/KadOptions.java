package org.texhnolyzze.kademlia;

import java.util.concurrent.TimeUnit;

public class KadOptions {

    private int k = 20;
    private int alpha = 3;
    private int port = 0;
    private boolean overwritePersistedPort = false;
    private int replacementCacheSize = k * 3;
    private long refreshIntervalMillis = TimeUnit.HOURS.toMillis(1);
    private long republishKeyIntervalMillis = TimeUnit.HOURS.toMillis(1);
    private long keyMaxLifetimeMillis = TimeUnit.DAYS.toMillis(1);
    private int kademliaPoolSize = Runtime.getRuntime().availableProcessors();
    private int grpcPoolSize = Runtime.getRuntime().availableProcessors();
    private long saveStateToFileIntervalMillis = 10_000;

    public KadOptions copy() {
        KadOptions copy = new KadOptions();
        copy.k = this.k;
        copy.alpha = this.alpha;
        copy.port = this.port;
        copy.overwritePersistedPort = this.overwritePersistedPort;
        copy.replacementCacheSize = this.replacementCacheSize;
        copy.refreshIntervalMillis = this.refreshIntervalMillis;
        copy.republishKeyIntervalMillis = this.republishKeyIntervalMillis;
        copy.keyMaxLifetimeMillis = this.keyMaxLifetimeMillis;
        copy.kademliaPoolSize = this.kademliaPoolSize;
        copy.grpcPoolSize = this.grpcPoolSize;
        copy.saveStateToFileIntervalMillis = this.saveStateToFileIntervalMillis;
        return copy;
    }

    public long getKeyMaxLifetimeMillis() {
        return keyMaxLifetimeMillis;
    }

    public KadOptions setKeyMaxLifetimeMillis(long keyMaxLifetimeMillis) {
        this.keyMaxLifetimeMillis = keyMaxLifetimeMillis;
        return this;
    }

    public long getRepublishKeyIntervalMillis() {
        return republishKeyIntervalMillis;
    }

    public KadOptions setRepublishKeyIntervalMillis(long republishKeyIntervalMillis) {
        this.republishKeyIntervalMillis = republishKeyIntervalMillis;
        return this;
    }

    public int getGrpcPoolSize() {
        return grpcPoolSize;
    }

    public KadOptions setGrpcPoolSize(int grpcPoolSize) {
        this.grpcPoolSize = grpcPoolSize;
        return this;
    }

    public int getKademliaPoolSize() {
        return kademliaPoolSize;
    }

    public KadOptions setKademliaPoolSize(int kademliaPoolSize) {
        this.kademliaPoolSize = kademliaPoolSize;
        return this;
    }

    public long getRefreshIntervalMillis() {
        return refreshIntervalMillis;
    }

    public KadOptions setRefreshIntervalMillis(long refreshIntervalMillis) {
        this.refreshIntervalMillis = refreshIntervalMillis;
        return this;
    }

    public int getReplacementCacheSize() {
        return replacementCacheSize;
    }

    public KadOptions setReplacementCacheSize(int replacementCacheSize) {
        this.replacementCacheSize = replacementCacheSize;
        return this;
    }

    public boolean isOverwritePersistedPort() {
        return overwritePersistedPort;
    }

    public KadOptions setOverwritePersistedPort(boolean overwritePersistedPort) {
        this.overwritePersistedPort = overwritePersistedPort;
        return this;
    }

    public int getPort() {
        return port;
    }

    public KadOptions setPort(int port) {
        this.port = port;
        return this;
    }

    public int getK() {
        return k;
    }

    public KadOptions setK(int k) {
        this.k = k;
        return this;
    }

    public int getAlpha() {
        return alpha;
    }

    public KadOptions setAlpha(int alpha) {
        this.alpha = alpha;
        return this;
    }

    public long getSaveStateToFileIntervalMillis() {
        return saveStateToFileIntervalMillis;
    }

    public KadOptions setSaveStateToFileIntervalMillis(long saveStateToFileIntervalMillis) {
        this.saveStateToFileIntervalMillis = saveStateToFileIntervalMillis;
        return this;
    }

}
