package redis.clients.jedis.ra;

import redis.clients.jedis.CommandObject;

public interface RAparallelExecutor extends AutoCloseable {

    <T> T executeCommand(RAparallelExecutor commandObject);
}