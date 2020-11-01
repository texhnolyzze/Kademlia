package org.texhnolyzze.kademlia;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KadIdTest {

    @Test
    void test() {
        KadId min = KadId.MIN;
        KadId max = KadId.MAX;
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
    }

}