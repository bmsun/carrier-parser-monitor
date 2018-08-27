package com.moxie.cloud.services.common.utils;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


public class TaskContextUtils {

    private static Map<String, Object> taskContextMap = new ConcurrentHashMap<>();

    /**
     * 上下文清楚消息
     * @param key
     */
    public static void clearTaskContext(String key) {
        if (StringUtils.isNotBlank(key)) {
            taskContextMap.remove(key);
        }
    }

    @PreDestroy
    public void destroy() {
        if (taskContextMap != null) {
            taskContextMap.clear();
        }
    }



}
