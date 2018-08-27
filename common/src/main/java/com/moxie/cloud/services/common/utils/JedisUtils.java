package com.moxie.cloud.services.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.net.SocketTimeoutException;

/***
 *@Description jedis实例获取和释放工具类
 * @author zhanghesheng
 * */
public class JedisUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(JedisUtils.class);

    public static JedisCommands getJedisCommands(JedisCluster jedisCluster, JedisPool jedisPool) {
        JedisCommands commands = null;
        if (jedisCluster == null) {
            Jedis jedis = jedisPool.getResource();
            commands = jedis;
        } else {
            commands = jedisCluster;
        }
        return commands;
    }

    public static void closeJedis(JedisCommands commands) {
        try {
        if (commands != null && commands instanceof Jedis) {
            Jedis jedis = (Jedis) commands;
            jedis.close();
        }
        } catch (Exception e) {
            LOGGER.error("释放连接异常", e);
        }
    }

    public static void logException(String message, JedisException e) {
        if(e.getCause() != null && e.getCause() instanceof SocketTimeoutException) {
            LOGGER.error(message, "操作redis超时");
        } else {
            LOGGER.error(message, e);
        }
    }
}
