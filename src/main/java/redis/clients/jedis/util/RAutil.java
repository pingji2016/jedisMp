package redis.clients.jedis.util;

import redis.clients.jedis.exceptions.JedisException;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class RAutil implements Map<byte[], byte[]>, Cloneable, Serializable {
    private static final long serialVersionUID = -6971431362627219416L;
    private final Map<RAutil.ByteArrayWrapper, byte[]> internalMap = new HashMap<>();

    @Override
    public void clear() {
        internalMap.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof byte[]) return internalMap.containsKey(new RAutil.ByteArrayWrapper((byte[]) key));
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return internalMap.containsValue(value);
    }

    @Override
    public Set<Map.Entry<byte[], byte[]>> entrySet() {
        Iterator<Map.Entry<RAutil.ByteArrayWrapper, byte[]>> iterator = internalMap.entrySet()
                .iterator();
        HashSet<Map.Entry<byte[], byte[]>> hashSet = new HashSet<>();
        while (iterator.hasNext()) {
            Map.Entry<RAutil.ByteArrayWrapper, byte[]> entry = iterator.next();
            hashSet.add(new RAutil.JedisByteEntry(entry.getKey().data, entry.getValue()));
        }
        return hashSet;
    }

    @Override
    public byte[] get(Object key) {
        if (key instanceof byte[]) return internalMap.get(new RAutil.ByteArrayWrapper((byte[]) key));
        return internalMap.get(key);
    }

    @Override
    public boolean isEmpty() {
        return internalMap.isEmpty();
    }

    @Override
    public Set<byte[]> keySet() {
        Set<byte[]> keySet = new HashSet<>();
        Iterator<RAutil.ByteArrayWrapper> iterator = internalMap.keySet().iterator();
        while (iterator.hasNext()) {
            keySet.add(iterator.next().data);
        }
        return keySet;
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        return internalMap.put(new RAutil.ByteArrayWrapper(key), value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void putAll(Map<? extends byte[], ? extends byte[]> m) {
        Iterator<?> iterator = m.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<? extends byte[], ? extends byte[]> next = (Map.Entry<? extends byte[], ? extends byte[]>) iterator
                    .next();
            internalMap.put(new RAutil.ByteArrayWrapper(next.getKey()), next.getValue());
        }
    }

    @Override
    public byte[] remove(Object key) {
        if (key instanceof byte[]) return internalMap.remove(new RAutil.ByteArrayWrapper((byte[]) key));
        return internalMap.remove(key);
    }

    @Override
    public int size() {
        return internalMap.size();
    }

    @Override
    public Collection<byte[]> values() {
        return internalMap.values();
    }

    private static final class ByteArrayWrapper implements Serializable {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            if (data == null) {
                throw new NullPointerException();
            }
            this.data = data;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) return false;
            if (other == this) return true;
            if (!(other instanceof RAutil.ByteArrayWrapper)) return false;

            return Arrays.equals(data, ((RAutil.ByteArrayWrapper) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

    private static final class JedisByteEntry implements Map.Entry<byte[], byte[]> {
        private byte[] value;
        private byte[] key;

        public JedisByteEntry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] getKey() {
            return this.key;
        }

        @Override
        public byte[] getValue() {
            return this.value;
        }

        @Override
        public byte[] setValue(byte[] value) {
            this.value = value;
            return value;
        }

    }

    public static boolean handleConnectionProblem(int attemptsLeft, int consecutiveConnectionFailures, Instant doneDeadline) {

        if (consecutiveConnectionFailures < 2) {
            return false;
        }

        //(getBackoffSleepMillis(attemptsLeft, doneDeadline));
        return true;
    }

    private static long getBackoffSleepMillis(int attemptsLeft, Instant deadline) {
        if (attemptsLeft <= 0) {
            return 0;
        }

        long millisLeft = Duration.between(Instant.now(), deadline).toMillis();
        if (millisLeft < 0) {
            throw new JedisException("Retry deadline exceeded.");
        }

        return millisLeft / (attemptsLeft * (attemptsLeft + 1));
    }
}
