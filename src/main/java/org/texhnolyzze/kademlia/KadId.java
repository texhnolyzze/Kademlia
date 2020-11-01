package org.texhnolyzze.kademlia;

import com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

class KadId implements Comparable<KadId> {

    static final int SIZE_BYTES = 4;
    static final KadId MIN = new KadId(new byte[SIZE_BYTES], true);
    static final KadId MAX = new KadId(new byte[SIZE_BYTES], true);
    static {
        for (int i = 0; i < SIZE_BYTES; i++) {
            MAX.raw[i] = (byte) 0xFF;
        }
    }

    private final byte[] raw;
    private BigInteger asBigInteger;
    private ByteString asByteString;

    KadId() {
        this.raw = new byte[SIZE_BYTES];
        ThreadLocalRandom.current().nextBytes(raw);
    }

    KadId(byte[] raw, boolean digested) {
        if (!digested) {
            this.raw = getDigest().digest(raw);
        } else
            this.raw = raw;
    }

    public KadId(ByteString nodeId) {
        this.raw = nodeId.toByteArray();
        this.asByteString = nodeId;
    }

    private BigInteger asBigInteger() {
        if (asBigInteger == null)
            asBigInteger = new BigInteger(1, raw);
        return asBigInteger;
    }

    private MessageDigest getDigest() {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new KademliaException("SHA-1 not supported", e);
        }
    }

    byte[] getRaw() {
        return raw;
    }

    KadId addOne() {
        return new KadId(getBytes(asBigInteger().add(BigInteger.ONE)), true);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (byte b : raw) {
            builder.append(String.format("%02X", b));
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KadId that = (KadId) o;
        return Arrays.equals(raw, that.raw);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(raw);
    }

    @Override
    public int compareTo(KadId o) {
        return compare(raw, o.raw);
    }

    byte[] distanceTo(KadId id) {
        byte[] dist = new byte[SIZE_BYTES];
        distanceTo(id, dist);
        return dist;
    }

    void distanceTo(KadId id, byte[] storeTo) {
        for (int i = 0; i < SIZE_BYTES; i++) {
            storeTo[i] = (byte) (raw[i] ^ id.raw[i]);
        }
    }

    static int compare(byte[] raw1, byte[] raw2) {
        for (int i = 0; i < SIZE_BYTES; i++) {
            if ((raw1[i] & 0xFF) < (raw2[i] & 0xFF))
                return -1;
            else if ((raw1[i] & 0xFF) > (raw2[i] & 0xFF))
                return +1;
        }
        return 0;
    }

    static KadId mid(KadId left, KadId right) {
        BigInteger bl = left.asBigInteger();
        BigInteger br = right.asBigInteger();
        BigInteger mid = br.subtract(bl).divide(BigInteger.TWO);
        KadId res = new KadId(getBytes(mid), true);
        res.asBigInteger = mid;
        return res;
    }

    private static byte[] getBytes(BigInteger bi) {
        byte[] bytes = bi.toByteArray();
        if (bytes.length != SIZE_BYTES) {
            int i = 0;
            while (i < bytes.length && bytes[i] == 0) i++;
            byte[] temp = new byte[SIZE_BYTES];
            for (int j = 0, k = bytes.length - 1; i < bytes.length; i++, j++, k--) {
                temp[SIZE_BYTES - j - 1] = bytes[k];
            }
            bytes = temp;
        }
        return bytes;
    }

    static KadId randomBetween(KadId min, KadId max) {
        double v = ThreadLocalRandom.current().nextDouble();
        BigDecimal sub = new BigDecimal(max.asBigInteger().subtract(min.asBigInteger()));
        BigDecimal minAsBigDecimal = new BigDecimal(min.asBigInteger());
        BigInteger res = minAsBigDecimal.add(sub.multiply(BigDecimal.valueOf(v))).toBigInteger();
        return new KadId(getBytes(res), true);
    }

    ByteString asByteString() {
        if (asByteString == null)
            asByteString = ByteString.copyFrom(raw);
        return asByteString;
    }

}
