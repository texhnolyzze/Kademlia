package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Just a small test to verify my understanding of Phaser class
 */
class PhaserTest {

    @Test
    void test() {
        Phaser phaser = new Phaser(1);
        final int n = 100;
        AtomicInteger integer = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            phaser.register();
            ForkJoinPool.commonPool().execute(() -> {
                phaser.register();
                phaser.arriveAndDeregister();
                integer.getAndIncrement();
                ForkJoinPool.commonPool().execute(() -> {
                    phaser.register();
                    phaser.arriveAndDeregister();
                    integer.getAndIncrement();
                    ForkJoinPool.commonPool().execute(() -> {
                        integer.getAndIncrement();
                        phaser.arriveAndDeregister();
                    });
                });
            });
        }
        phaser.arriveAndAwaitAdvance();
        assertEquals(n * 3, integer.get());
    }

}
