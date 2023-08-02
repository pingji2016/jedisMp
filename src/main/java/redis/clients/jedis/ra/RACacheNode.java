package redis.clients.jedis.ra;

import redis.clients.jedis.CommandObject;
import redis.clients.jedis.Connection;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.providers.ConnectionProvider;
import redis.clients.jedis.util.RAutil;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

// 每个节点构建新的cache
public class RACacheNode<K, V> {
    private long timestamp;
    private List<TransactionEntry<K, V>> transactionEntryList;
    protected final RAparallelExecutor executor;
    protected final ConnectionProvider provider;
    private final int maxAttempts = 5;
    protected final Duration dealtTime;


    public RACacheNode(long timestamp, RAparallelExecutor executor, ConnectionProvider provider, Duration dealtTime) {
        this.timestamp = timestamp;
        this.executor = executor;
        this.provider = provider;
        this.dealtTime = dealtTime;
    }

    public RACacheNode(long timestamp, ConnectionProvider provider, Duration dealtTime) {
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

    public RACacheNode(List<TransactionEntry<K, V>> transactionEntryList, RAparallelExecutorr executor, ConnectionProvider provider, Duration dealtTime) {
        this.transactionEntryList = transactionEntryList;
        this.executor = executor;
        this.provider = provider;
        this.dealtTime = dealtTime;
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

    public <T> T readTxn(CommandObject<T> commandObject) {

        Instant deadline = Instant.now().plus(dealtTime);

        int consecutiveConnectionFailures = 0;
        JedisException lastException = null;
        for (int attemptsLeft = this.maxAttempts; attemptsLeft > 0; attemptsLeft--) {
            Connection connection = null;
            try {
                connection = provider.getConnection(commandObject.getArguments());

                return execute(connection, commandObject);

            } catch (JedisConnectionException jce) {
                lastException = jce;
                ++consecutiveConnectionFailures;
                //log.debug("Failed connecting to Redis: {}", connection, jce);
                // "- 1" because we just did one, but the attemptsLeft counter hasn't been decremented yet
                boolean reset = RAutil.handleConnectionProblem(attemptsLeft - 1, consecutiveConnectionFailures, deadline);//handleConnectionProblem(attemptsLeft - 1, consecutiveConnectionFailures, deadline);
                if (reset) {
                    consecutiveConnectionFailures = 0;
                }
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            if (Instant.now().isAfter(deadline)) {
                throw new JedisException("Retry deadline exceeded.");
            }
        }

        JedisException maxAttemptsException = new JedisException("No more attempts left.");
        maxAttemptsException.addSuppressed(lastException);
        throw maxAttemptsException;
    }

    protected <T> T execute(Connection connection, CommandObject<T> commandObject) {
        return connection.executeCommand(commandObject);
    }
}
