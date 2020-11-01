package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class KadIdTest {

    @Test
    void test() {
        final KadId min = KadId.MIN;
        final KadId max = KadId.MAX;
        KadId mid = KadId.mid(min, max);
        assertTrue(min.compareTo(mid) < 0);
        assertTrue(mid.compareTo(max) < 0);
        for (int i = 0; i < 1000; i++)
            mid = KadId.mid(min, mid);
        assertEquals(BigInteger.ZERO, new BigInteger(1, mid.getRaw()));
        KadId id = min.addOne();
        assertEquals(BigInteger.ONE, new BigInteger(1, id.getRaw()));
        byte[] dist = new byte[KadId.SIZE_BYTES];
        id.distanceTo(min, dist);
        assertEquals(BigInteger.ONE, new BigInteger(1, dist));
        mid = KadId.mid(min, max);
        for (int i = 0; i < 1000; i++) {
            mid = KadId.mid(mid, max);
        }
        assertEquals(mid.asBigInteger().add(BigInteger.ONE), max.asBigInteger());
        assertEquals(0, mid.compareTo(mid));
        assertEquals(mid, mid);
        assertNotEquals(mid, max);
        dist = mid.distanceTo(max);
        assertEquals(BigInteger.ONE, new BigInteger(1, dist));
        for (int i = 0; i < 1000; i++) {
            KadId random = KadId.randomBetween(min, max);
            assertTrue(min.compareTo(random) <= 0);
            assertTrue(random.compareTo(max) <= 0);
        }
    }

}