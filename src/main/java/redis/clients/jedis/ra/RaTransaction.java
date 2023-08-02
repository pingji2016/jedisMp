package redis.clients.jedis.ra;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RaTransaction extends Transaction {

    public RaTransaction(Jedis jedis) {
        super(jedis);
    }

    public RaTransaction(Connection connection) {
        super(connection);
    }

    public RaTransaction(Connection connection, boolean doMulti) {
        super(connection, doMulti);
    }
}
