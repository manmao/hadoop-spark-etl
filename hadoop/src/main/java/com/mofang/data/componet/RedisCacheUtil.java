package com.mofang.data.componet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;



public class RedisCacheUtil {

    private static StringRedisTemplate redisTemplate;

    public RedisCacheUtil() {
        
    }
    
    public static void init(StringRedisTemplate stringRedisTemplate) {
        redisTemplate=stringRedisTemplate;
    }

    /* ----------- common --------- */
    public static Collection<String> keys(String pattern) {
        return redisTemplate.keys(pattern);
    }

    public static boolean exists(String key) {
        return redisTemplate.hasKey(key);
    }

    public static void expire(String key, Long timeout, TimeUnit unit) {
        redisTemplate.expire(key, timeout, unit);
    }

    public static void del(String key) {
        redisTemplate.delete(key);
    }

    public static void del(Collection<String> key) {
        redisTemplate.delete(key);
    }

    /* ----------- string --------- */
    public static <T> T get(String key, Class<T> clazz) {
        String value = redisTemplate.opsForValue().get(key);
        return parseJson(value, clazz);
    }

    public static <T> List<T> mget(Collection<String> keys, Class<T> clazz) {
        List<String> values = redisTemplate.opsForValue().multiGet(keys);
        return parseJsonList(values, clazz);
    }

    public static <T> void set(String key, T obj) {
        if (obj == null) {
            return;
        }
        redisTemplate.opsForValue().set(key, toJson(obj));
    }

    public static <T> void set(String key, T obj, Long timeout, TimeUnit unit) {
        if (obj == null) {
            return;
        }

        String value = toJson(obj);
        if (timeout != null) {
            redisTemplate.opsForValue().set(key, value, timeout, unit);
        } else {
            redisTemplate.opsForValue().set(key, value);
        }
    }

    public static <T> T getSet(String key, T obj, Class<T> clazz) {
        if (obj == null) {
            return get(key, clazz);
        }

        String value = redisTemplate.opsForValue().getAndSet(key, toJson(obj));
        return parseJson(value, clazz);
    }

    public static int incr(String key, int delta) {
        Long value = redisTemplate.opsForValue().increment(key, delta);
        return value.intValue();
    }

    public static int decr(String key, int delta) {
        Long value = redisTemplate.opsForValue().increment(key, -delta);
        return value.intValue();
    }

    /* ----------- list --------- */
    public static int llen(String key) {
        return redisTemplate.opsForList().size(key).intValue();
    }

    public static <T> List<T> lrange(String key, long start, long end, Class<T> clazz) {
        List<String> list = redisTemplate.opsForList().range(key, start, end);
        return parseJsonList(list, clazz);
    }

    public static <T> void lpush(String key, T obj) {
        if (obj == null) {
            return;
        }

        redisTemplate.opsForList().leftPush(key, toJson(obj));
    }

    public static void rpush(String key, Collection<?> values, Long timeout, TimeUnit unit) {
        if (values == null || values.isEmpty()) {
            return;
        }

        redisTemplate.opsForList().rightPushAll(key, toJsonList(values));
        if (timeout != null) {
            redisTemplate.expire(key, timeout, unit);
        }
    }

    public static <T> T lpop(String key, Class<T> clazz) {
        String value = redisTemplate.opsForList().leftPop(key);
        return parseJson(value, clazz);
    }

    public static <T> T rpop(String key, Class<T> clazz) {
        String value = redisTemplate.opsForList().rightPop(key);
        return parseJson(value, clazz);
    }

    public static void lrem(String key, int count, Object obj) {
        if (obj == null) {
            return;
        }
        redisTemplate.opsForList().remove(key, count, toJson(obj));
    }

    /* ----------- zset --------- */
    public static int zcard(String key) {
        return redisTemplate.opsForZSet().zCard(key).intValue();
    }

    public static <T> List<T> zrange(String key, long start, long end, Class<T> clazz) {
        Set<String> set = redisTemplate.opsForZSet().range(key, start, end);
        return setToList(set, clazz);
    }

    public static <T> List<TypedTuple<T>> zrangeWithScores(String key, long start, long end, Class<T> clazz) {
        Set<TypedTuple<String>> set = redisTemplate.opsForZSet().rangeWithScores(key, start, end);
        return tupleSetToList(set, clazz);
    }

    public static <T> List<TypedTuple<T>> zrangeByScoreWithScores(String key, double min, double max, long offset,
                                                                  long count, Class<T> clazz) {
        Set<TypedTuple<String>> set =
                redisTemplate.opsForZSet().rangeByScoreWithScores(key, min, max, offset, count);
        return tupleSetToList(set, clazz);
    }

    public static <T> List<TypedTuple<T>> zrevrangeByScoreWithScores(String key, double min, double max, long offset,
                                                                     long count, Class<T> clazz) {
        Set<TypedTuple<String>> set =
                redisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, min, max, offset, count);
        return tupleSetToList(set, clazz);
    }

    private static <T> List<T> setToList(Set<String> set, Class<T> clazz) {
        if (set == null) {
            return null;
        }
        List<T> list = new ArrayList<T>();
        for (String str : set) {
            T obj = parseJson(str, clazz);
            if (obj != null) {
                list.add(obj);
            }
        }
        return list;
    }

    private static <T> List<TypedTuple<T>> tupleSetToList(Set<TypedTuple<String>> set, Class<T> clazz) {
        if (set == null) {
            return null;
        }
        List<TypedTuple<T>> list = new ArrayList<TypedTuple<T>>();
        for (TypedTuple<String> tuple : set) {
            T obj = parseJson(tuple.getValue(), clazz);
            if (obj != null) {
                list.add(new DefaultTypedTuple<T>(obj, tuple.getScore()));
            }
        }
        return list;
    }

    public static void zadd(String key, Object obj, double score) {
        if (obj == null) {
            return;
        }
        redisTemplate.opsForZSet().add(key, toJson(obj), score);
    }

    public static <T> void zadd(String key, List<TypedTuple<T>> tupleList) {
        if (tupleList == null || tupleList.isEmpty()) {
            return;
        }
        Set<TypedTuple<String>> tupleSet = toTupleSet(tupleList);
        redisTemplate.opsForZSet().add(key, tupleSet);
    }

    private static <T> Set<TypedTuple<String>> toTupleSet(List<TypedTuple<T>> tupleList) {
        Set<TypedTuple<String>> tupleSet = new LinkedHashSet<TypedTuple<String>>();
        for (TypedTuple<?> t : tupleList) {
            tupleSet.add(new DefaultTypedTuple<String>(toJson(t.getValue()), t.getScore()));
        }
        return tupleSet;
    }

    public static void zrem(String key, Object obj) {
        if (obj == null) {
            return;
        }
        redisTemplate.opsForZSet().remove(key, toJson(obj));
    }

    public static void zremRangeByScore(String key, Double min, Double max) {
        redisTemplate.opsForZSet().removeRangeByScore(key, min, max);
    }

    public static void unionStore(String destKey, Collection<String> keys, Long timeout, TimeUnit unit) {
        if (keys == null || keys.isEmpty()) {
            return;
        }

        Object[] keyArr = keys.toArray();
        String key = (String) keyArr[0];

        Collection<String> otherKeys = new ArrayList<String>(keys.size() - 1);
        for (int i = 1; i < keyArr.length; i++) {
            otherKeys.add((String) keyArr[i]);
        }

        redisTemplate.opsForZSet().unionAndStore(key, otherKeys, destKey);
        if (timeout != null) {
            redisTemplate.expire(destKey, timeout, unit);
        }
    }

    /* ----------- hash --------- */
    public static <T> T hget(String key, String field, Class<T> clazz) {
        String value = (String) redisTemplate.opsForHash().get(key, field);
        return parseJson(value, clazz);
    }

    public static <T> void hset(String key, String field, T obj) {
        if (obj == null) {
            return;
        }
        redisTemplate.opsForHash().put(key, field, toJson(obj));
    }

    /* ----------- tool methods --------- */
    public static String toJson(Object obj) {
        return JSON.toJSONString(obj, SerializerFeature.SortField);
    }

    public static <T> T parseJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

    public static List<String> toJsonList(Collection<?> values) {
        if (values == null) {
            return null;
        }

        List<String> result = new ArrayList<String>();
        for (Object obj : values) {
            result.add(toJson(obj));
        }
        return result;
    }

    public static <T> List<T> parseJsonList(List<String> list, Class<T> clazz) {
        if (list == null) {
            return null;
        }

        List<T> result = new ArrayList<T>();
        for (String s : list) {
            result.add(parseJson(s, clazz));
        }
        return result;
    }
}
