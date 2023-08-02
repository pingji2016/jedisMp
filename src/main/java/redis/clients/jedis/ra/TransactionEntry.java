package redis.clients.jedis.ra;

import redis.clients.jedis.util.KeyValue;

import java.util.HashMap;

public class TransactionEntry<K, V> extends KeyValue<K, V> {
    public TransactionEntry(K key, V value) {
        super(key, value);
    }
    HashMap<K, V> updatedData = new HashMap<>();
}
