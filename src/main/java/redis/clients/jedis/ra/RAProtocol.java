package redis.clients.jedis.ra;


import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.providers.ConnectionProvider;
import redis.clients.jedis.util.RAutil;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class RAProtocol<K, V> {
    private long timestamp;
    private List<TransactionEntry<K, V>> transactionEntryList;
    protected final RAparallelExecutor executor;
    protected final ConnectionProvider provider;
    private final int maxAttempts = 5;
    protected final Duration dealtTime;
    private Object Exception;
    HashMap<K>

    public RAProtocol(long timestamp, RAparallelExecutor executor, ConnectionProvider provider, Duration dealtTime) {
        this.timestamp = timestamp;
        this.executor = executor;
        this.provider = provider;
        this.dealtTime = dealtTime;
    }

    public RAProtocol(long timestamp, ConnectionProvider provider, Duration dealtTime) {
        this.timestamp = timestamp;
        this.provider = provider;
        this.dealtTime = dealtTime;
        this.executor = new RAparallelExecutor() {
            @Override
            public void close() throws Exception {

            }

            @Override
            public <T> T executeCommand(RAparallelExecutor commandObject) {
                return null;
            }
        };
    }

    public void initData(TransactionEntry<K, V> entry) {
        transactionEntryList.add(entry);
    }

    public void refreshData() {
        for (TransactionEntry<K, V> item : transactionEntryList) {
            if (Long.parseLong((String) item.getKey()) > timestamp) {
                //discord
                transactionEntryList.remove(item);
            }
        }
    }



//        Instant deadline = Instant.now().plus(dealtTime);
//
//        int consecutiveConnectionFailures = 0;
//        JedisException lastException = null;
//        for (int attemptsLeft = this.maxAttempts; attemptsLeft > 0; attemptsLeft--) {
//            Connection connection = null;
//            try {
//                connection = provider.getConnection(commandObject.getArguments());
//
//                return execute(connection, commandObject);
//
//            } catch (JedisConnectionException jce) {
//                lastException = jce;
//                ++consecutiveConnectionFailures;
//                //log.debug("Failed connecting to Redis: {}", connection, jce);
//                // "- 1" because we just did one, but the attemptsLeft counter hasn't been decremented yet
//                boolean reset = RAutil.handleConnectionProblem(attemptsLeft - 1, consecutiveConnectionFailures, deadline);//handleConnectionProblem(attemptsLeft - 1, consecutiveConnectionFailures, deadline);
//                if (reset) {
//                    consecutiveConnectionFailures = 0;
//                }
//            } finally {
//                if (connection != null) {
//                    connection.close();
//                }
//            }
//            if (Instant.now().isAfter(deadline)) {
//                throw new JedisException("Retry deadline exceeded.");
//            }
//        }
//
//        JedisException maxAttemptsException = new JedisException("No more attempts left.");
//        maxAttemptsException.addSuppressed(lastException);
//        throw maxAttemptsException;

    public <T> T readTxn(CommandObject<T> commandObject) {
        Connection connection = provider.getConnection(commandObject.getArguments());
        Response<T> response = execute(connection, commandObject);
        if (AtomicCheck(response)) {
            return connection;
        }
        Long preTime = System.currentTimeMillis();
        Long timeNow = System.currentTimeMillis();
        HashMap<T, Long> tsLast = new HashMap<T, Long>() {};
        while (timeNow - preTime < timeout) {//未超时
            try {
                for (item : readSet) {
                    for (md : response.metaData) {
                        if (md.key.equals(item) && !response.trasactionList.empty()) {
                            tsLast.put(item, Math.max(tsLast.get(md.key), response.trasactionList.get(md.key).getVersion()));
                        }
                    }
                }
                HashMap<T, Long> needVersion = new HashMap<T, Long>() {};
                HashMap<T, Long> newerVersion = new HashMap<T, Long>() {};
                for (T item : readSet) {
                    if (tsLast.get(item).getVersion() > response.metaData.get(item).getVersion()) {
                        needVersion.put(item, tsLast.get(item));
                    }
                }
                for (Map.Entry<T,Long> entry: needVersion.entrySet()) {
                    Connection connection = provider.getConnection(commandObject.getArguments());
                    Response<T> responseNewer = execute(connection, commandObject.getBuilder(item, needVersion));
                    if (entry.getValue() != responseNewer.metaData.get(entry.getKey()).getVersion()) {
                        newerVersion.put(entry.getKey(), responseNewer.metaData.get(entry.getKey()).getVersion();
                    }
                }
                if (newerVersion.isEmpty()) {
                    return responseNewer;
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            timeNow = System.currentTimeMillis();
        }
        return TIMEOUT;
    }

    protected <T> T execute(Connection connection, CommandObject<T> commandObject) {
        return connection.executeCommand(commandObject);
    }
}
