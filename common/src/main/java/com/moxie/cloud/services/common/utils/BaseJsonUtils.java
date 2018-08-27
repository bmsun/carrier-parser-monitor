package com.moxie.cloud.services.common.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.text.SimpleDateFormat;
import java.util.Map;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Slf4j
public class BaseJsonUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
    }

    public static ObjectMapper defaultMapper() {
        return mapper;
    }

    public static <T> T readValue(String s, Class<T> cls) {
        Preconditions.checkArgument(StringUtils.isNotBlank(s), "字符串不能为空");
        Preconditions.checkArgument(cls != null, "Class类型不能为空");

        try {
            return mapper.readValue(s, cls);
        } catch (Throwable e) {
            log.error("无法将[{}]转换为类型为[{}]的对象: {}", s, cls.getSimpleName(), ExceptionUtils.getStackTrace(e));
            return null;
        }
    }

    public static <T> T readValue(Map<String, Object> map, Class<T> cls) {
        Preconditions.checkArgument(map != null, "map不能为null");
        Preconditions.checkArgument(cls != null, "Class类型不能为空");

        try {
            return mapper.convertValue(map, cls);
        } catch (Throwable e) {
            log.error("无法将Map[{}]转换为类型为[{}]的对象: {}", map, cls.getSimpleName(), ExceptionUtils.getStackTrace(e));
            return null;
        }
    }

    public static String writeValue(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return mapper.writeValueAsString(obj);
        } catch (Throwable e) {
            log.error("json转换异常", e);
            return null;
        }
    }

    public static Object valueFromJsonKey(String json, String key) {
        Preconditions.checkArgument(StringUtils.isNotBlank(json), "json内容不能为空");
        Preconditions.checkArgument(StringUtils.isNotBlank(key), "key不能为空");
        Map<String, Object> map = readValue(json, Map.class);
        return map.get(key);
    }
}
