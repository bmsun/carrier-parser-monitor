package com.moxie.cloud.services.server.service;


import com.moxie.cloud.carrier.entity.CarrierPropertiesEntity;
import com.moxie.cloud.services.common.MonitorServiceConstants;
import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.utils.JedisUtils;
import com.moxie.cloud.services.server.cache.CarrierPropertiesCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class RedisService {

    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "EX";
    private static final Long RELEASE_SUCCESS = 1L;

    @Autowired
    private JedisCluster jedisCluster;

    @Autowired
    private JedisPool jedisPool;

    @Autowired
    private CarrierPropertiesCache carrierPropertiesCache;

    protected JedisCommands getJedis() {
        return JedisUtils.getJedisCommands(jedisCluster, jedisPool);
    }

    public String getSet(String key,String value){
        JedisCommands jedis =null;
        try {
            jedis=getJedis();
            return jedis.getSet(key,value);
        } catch (Exception e) {
            log.error("Redis getSet({}) exception:{}", key , ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(jedis);
        }
    }

    public String set(String key,String value){
        JedisCommands jedis =null;
        try {
            jedis=getJedis();
            return jedis.set(key,value);
        } catch (Exception e) {
            log.error("Redis set({}) exception:{}", key , ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(jedis);
        }
    }


    public String get(String key){
        JedisCommands jedis =null;
        try {
            jedis=getJedis();
            return jedis.get(key);
        } catch (Exception e) {
            log.error("Redis set({}) exception:{}", key , ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(jedis);
        }
    }
    /**
     *
     * @param key
     * @param score
     * @param value
     * @return
     */
    public Long zadd(String key, double score, String value) {
        JedisCommands commands = null;
        try {
            commands = getJedis();
            return commands.zadd(key, score, value);
        } catch (Exception e) {
            log.error("Redis commands.del({}) exception:{}", key , ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }

    /**
     * 该命令用于在 key 存在时删除 key
     * @param key
     * @return
     */
    public Long del(String key) {
        JedisCommands commands = null;
        try {
            commands = getJedis();
            return commands.del(key);
        } catch (Exception e) {
            log.error("Redis del({}) exception:{}", key , ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }

    public Long expire(String key, int time) {
        JedisCommands commands = null;
        try {
            commands =  getJedis();
            return commands.expire(key, time);
        } catch (Exception e) {
            log.error("Redis expire({}) exception:{}", key , ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }


    /**
     * 为哈希表 key 中的指定字段的整数值加上增量 increment 。
     * @param key
     * @param field
     * @param increment
     * @return
     */
    public Long hIncrby(String key, String field, long increment) {
        JedisCommands commands = null;
        try {
            commands = getJedis();
            return commands.hincrBy(key, field, increment);
        } catch (Exception e) {
            log.error("Redis hIncrby({},{},{}) exception:{}", key , field, increment, ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }


    /**
     *
     * @param key
     * @return key剩余存活时间 单位：秒
     */
    public Long ttl(String key) {
        JedisCommands commands = null;
        try {
            commands = getJedis();
            return commands.ttl(key);
        } catch (Exception e) {
            log.error("Redis ttl({}) exception:{}", key ,ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }


    /**
     * 检查给定 key 是否存在
     * @param key
     * @return
     */
    public Boolean exists(String key) {
        JedisCommands commands = null;
        try {
            commands = getJedis();
            return commands.exists(key);
        } catch (Exception e) {
            log.error("Redis exists({},{}) exception:{}", key , ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }

    public void init(String key,String field, int expireTime){

        this.hSet(key,field,"1");
        this.expire(key,expireTime);
    }


    /**
     * 将哈希表 key 中的字段 field 的值设为 value
     * @param key
     * @param field
     * @param value
     * @return
     */
    public Long hSet(String key, String field, String value) {
        JedisCommands commands = null;
        try {
            commands = getJedis();

            return commands.hset(key, field, value);
        } catch (Exception e) {
            log.error("Redis hset({},{},{}) exception:{}", key , field, value, ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }

    /**
     * 将哈希表 key 中的字段 field 的值设为 value
     * @param key
     * @param value
     * @return
     */
    public String hmset(String key, Map<String, String> value) {
        JedisCommands commands = null;
        try {
            commands = getJedis();
            return commands.hmset(key, value);
        } catch (Exception e) {
            log.error("Redis hmset({}, {}) exception:{}", key , value, ExceptionUtils.getStackTrace(e));
            return null;
        } finally {
            JedisUtils.closeJedis(commands);
        }
    }

    /**
     * 尝试获取分布式锁
     * @param lockKey 锁
     * @param requestId 请求标识
     * @param expireTime 超期时间
     * @return 是否获取成功
     *
     * 第一个为key，我们使用key来当锁，因为key是唯一的。
     * 第二个为value，我们传的是requestId，很多童鞋可能不明白，有key作为锁不就够了吗，为什么还要用到value？原因就是分布式锁要满足第四个条件解铃还须系铃人，通过给value赋值为requestId，我们就知道这把锁是哪个请求加的了，在解锁的时候就可以有依据。requestId可以使用UUID.randomUUID().toString()方法生成。
     * 第三个为nxxx，这个参数我们填的是NX，意思是SET IF NOT EXIST，即当key不存在时，我们进行set操作；若key已经存在，则不做任何操作；
     * 第四个为expx，这个参数我们传的是PX，意思是我们要给这个key加一个过期的设置，具体时间由第五个参数决定。
     * 第五个为time，与第四个参数相呼应，代表key的过期时间。
     */
    public  boolean tryGetDistributedLock( String lockKey, String requestId, int expireTime) {
        JedisCommands commands =null;
        try {
            commands =  getJedis();
            String result = commands.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);

            if (LOCK_SUCCESS.equals(result)) {
                return true;
            }
            return false;
        } catch (Exception e) {
            log.error("Redis tryGetDistributedLock({}, {}) exception:{}",lockKey,requestId, ExceptionUtils.getStackTrace(e));
            return false;
        }finally {
            JedisUtils.closeJedis(commands);
        }

    }


    /**
     * 释放分布式锁
     * @param lockKey 锁
     * @param requestId 请求标识
     * @return 是否释放成功
     */
    public  boolean releaseDistributedLock( String lockKey, String requestId) {
        JedisCommands jedisCommands = getJedis();
        ScriptingCommands jedis=null;
        if (jedisCommands instanceof Jedis||jedisCommands instanceof JedisCluster) {
            jedis = (ScriptingCommands) jedisCommands;
        }
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));

        if (RELEASE_SUCCESS.equals(result)) {
            return true;
        }
        return false;

    }
}
