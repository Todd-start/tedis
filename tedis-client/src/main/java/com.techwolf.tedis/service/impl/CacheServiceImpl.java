package com.techwolf.tedis.service.impl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.techwolf.tedis.cluster.TedisClusterClient;
import com.techwolf.tedis.codec.*;
import com.techwolf.tedis.service.CacheService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisClusterScriptingCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.MultiKeyJedisClusterCommands;
import redis.clients.jedis.Tuple;

import java.util.*;

import static com.techwolf.tedis.util.NameSpaceUtil.combineKeyNameSpace;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public class CacheServiceImpl implements CacheService {

    private static final Logger logger = LoggerFactory.getLogger(CacheServiceImpl.class);

    private static final String DEFAULT_NAMESPACE = "boss";

    private String nameSpace = DEFAULT_NAMESPACE;

    private static final int MAX_HASH_HEY_FILED_THRESHOLD = 5000;

    private static final int MAX_HASH_HEY_THRESHOLD = 3000;

    private JedisCommands jedisClient;

    private MultiKeyJedisClusterCommands multiKeyClient;

    private JedisClusterScriptingCommands scriptClient;

    public void setJedisClient(JedisCommands jedisClient) {
        this.jedisClient = jedisClient;
    }

    public void setMultiKeyClient(MultiKeyJedisClusterCommands multiKeyClient) {
        this.multiKeyClient = multiKeyClient;
    }

    public void setScriptClient(JedisClusterScriptingCommands scriptClient) {
        this.scriptClient = scriptClient;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    @Override
    public Long del(String key) {
        return jedisClient.del(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public boolean exists(String key) {
        return jedisClient.exists(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public Map<String, Boolean> mExists(List<String> keyList) {
        if (multiKeyClient instanceof TedisClusterClient) {
            Map<String, Boolean> combineKeyResultMap = ((TedisClusterClient) multiKeyClient).mExists(getCombineKeyList(keyList));
            Map<String, Boolean> resultMap = MapUtils.createHashMap(keyList.size());
            for (int i = 0; i < keyList.size(); ++i) {
                String key = keyList.get(i);
                String combineKey = combineKeyNameSpace(nameSpace, key);
                resultMap.put(key, combineKeyResultMap.get(combineKey));
            }
            return resultMap;
        }
        return MapUtils.createEmptyMap();
    }

    private List<String> getCombineKeyList(List<String> keyList) {
        List<String> combineKeys = new ArrayList<String>(keyList.size());
        for (int i = 0; i < keyList.size(); i++) {
            String key = keyList.get(i);
            combineKeys.add(combineKeyNameSpace(nameSpace, key));
        }
        return combineKeys;
    }

    @Override
    public Long setnx(String key, String value) {
        return jedisClient.setnx(combineKeyNameSpace(nameSpace, key), value);
    }

    @Override
    public void expire(String key, int seconds) {
        jedisClient.expire(combineKeyNameSpace(nameSpace, key), seconds);
    }

    @Override
    public Long expire(List<String> keyList, int seconds) {
        if (multiKeyClient instanceof TedisClusterClient) {
            return ((TedisClusterClient) multiKeyClient).expire(getCombineKeyList(keyList), seconds);
        }
        return 0L;
    }

    @Override
    public long ttl(String key) {
        return jedisClient.ttl(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public Long getLong(String key) {
        String value = get(key);
        return NumberUtils.toLong(value, 0);
    }

    @Override
    public void setLong(String key, long value) {
        set(key, String.valueOf(value));
    }

    @Override
    public void setLong(String key, long value, int seconds) {
        jedisClient.setex(combineKeyNameSpace(nameSpace, key), seconds, String.valueOf(value));
    }

    @Override
    public void zadd(String key, int score, String member) {
        zadd(key, score, member);
    }

    @Override
    public void zadd(String key, Map<String, Double> scoreMemers) {
        jedisClient.zadd(combineKeyNameSpace(nameSpace, key), scoreMemers);
    }

    @Override
    public void zadd(String key, long score, String member) {
        jedisClient.zadd(combineKeyNameSpace(nameSpace, key), score, member);
    }

    @Override
    public int zrank(String key, String member) {
        return jedisClient.zrank(combineKeyNameSpace(nameSpace, key), member).intValue();
    }

    @Override
    public int zcard(String key) {
        return jedisClient.zcard(combineKeyNameSpace(nameSpace, key)).intValue();
    }

    @Override
    public Double zscore(String key, String member) {
        return jedisClient.zscore(combineKeyNameSpace(nameSpace, key), member);
    }

    @Override
    public int zremrangeByRank(String key, int start, int end) {
        return jedisClient.zremrangeByRank(combineKeyNameSpace(nameSpace, key), start, end).intValue();
    }

    @Override
    public int zrevrank(String key, String member) {
        return jedisClient.zrevrank(combineKeyNameSpace(nameSpace, key), member).intValue();
    }

    @Override
    public List<String> zrevrange(String key, int start, int stop) {
        return new ArrayList<>(jedisClient.zrevrange(combineKeyNameSpace(nameSpace, key), start, stop));
    }

    @Override
    public List<String> zrange(String key, int start, int stop) {
        return new ArrayList<>(jedisClient.zrange(combineKeyNameSpace(nameSpace, key), start, stop));
    }

    @Override
    public List<Tuple> zrevrangeWithScore(String key, int start, int stop) {
        return new ArrayList<>(jedisClient.zrevrangeWithScores(combineKeyNameSpace(nameSpace, key), start, stop));
    }

    @Override
    public List<Tuple> zrangeWithScore(String key, int start, int stop) {
        return new ArrayList<>(jedisClient.zrangeWithScores(combineKeyNameSpace(nameSpace, key), start, stop));
    }

    @Override
    public List<String> zrangeByScore(String key, double min, double max) {
        return new ArrayList<>(jedisClient.zrangeByScore(combineKeyNameSpace(nameSpace, key), min, max));
    }

    @Override
    public List<String> zrangeByScore(String key, String min, String max) {
        return new ArrayList<>(jedisClient.zrangeByScore(combineKeyNameSpace(nameSpace, key), min, max));
    }

    @Override
    public List<String> zrevrangeByScore(String key, double max, double min) {
        return new ArrayList<>(jedisClient.zrevrangeByScore(combineKeyNameSpace(nameSpace, key), max, min));
    }

    @Override
    public List<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return new ArrayList<>(jedisClient.zrangeByScore(combineKeyNameSpace(nameSpace, key), min, max));
    }

    @Override
    public List<String> zrevrangeByScore(String key, String max, String min) {
        return new ArrayList<>(jedisClient.zrevrangeByScore(combineKeyNameSpace(nameSpace, key), max, min));
    }

    @Override
    public List<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return new ArrayList<>(jedisClient.zrangeByScore(combineKeyNameSpace(nameSpace, key), min, max, offset, count));
    }

    @Override
    public List<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return new ArrayList<>(jedisClient.zrevrangeByScore(combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    @Override
    public List<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return new ArrayList<>(jedisClient.zrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), min, max));
    }

    @Override
    public List<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return new ArrayList<>(jedisClient.zrevrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), max, min));
    }

    @Override
    public List<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return new ArrayList<>(jedisClient.zrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    @Override
    public List<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return new ArrayList<>(jedisClient.zrevrangeByScore(combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    @Override
    public List<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return new ArrayList<>(jedisClient.zrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), min, max));
    }

    @Override
    public List<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return new ArrayList<>(jedisClient.zrevrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), max, min));
    }

    @Override
    public List<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return new ArrayList<>(jedisClient.zrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), min, max, offset, count));
    }

    @Override
    public List<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return new ArrayList<>(jedisClient.zrevrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    @Override
    public List<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return new ArrayList<>(jedisClient.zrevrangeByScoreWithScores(combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return jedisClient.zremrangeByScore(combineKeyNameSpace(nameSpace, key), start, end);
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return jedisClient.zremrangeByScore(combineKeyNameSpace(nameSpace, key), start, end);
    }

    @Override
    public void zrem(String key, String member) {
        jedisClient.zrem(combineKeyNameSpace(nameSpace, key), member);
    }

    @Override
    public void zrem(String key, String... members) {
        jedisClient.zrem(combineKeyNameSpace(nameSpace, key), members);
    }

    @Override
    public double zincrby(String key, double score, String member) {
        return jedisClient.zincrby(combineKeyNameSpace(nameSpace, key), score, member);
    }

    @Override
    public Long increment(String key) {
        return jedisClient.incr(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public Long incrementBy(String key, long delta) {
        return jedisClient.incrBy(combineKeyNameSpace(nameSpace, key), delta);
    }

    @Override
    public Long decrement(String key) {
        return jedisClient.decr(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public Long decrementBy(String key, long decrement) {
        return jedisClient.decrBy(combineKeyNameSpace(nameSpace, key), decrement);
    }

    @Override
    public List<Long> getLongList(String key) {
        List<String> bytesList = jedisClient.lrange(combineKeyNameSpace(nameSpace, key), 0, -1);
        if (bytesList == null) {
            return new ArrayList<>(0);
        }
        return Lists.newArrayList(Lists.transform(bytesList, new Function<String, Long>() {
            public Long apply(String s) {
                return NumberUtils.toLong(s, 0);
            }
        }));
    }

    @Override
    public void setLongList(String key, List<Long> list) {
        throw new UnsupportedOperationException("setLongList(String key, List<Long> list)!!!");
    }

    @Override
    public void setLongList(String key, List<Long> list, int seconds) {
        throw new UnsupportedOperationException("setLongList(String key, List<Long> list, int seconds)!!!");
    }

    @Override
    public void addLongElement(String key, long number) {
        jedisClient.rpush(combineKeyNameSpace(nameSpace, key), String.valueOf(number));
    }

    @Override
    public void addLongList(String key, List<Long> number) {
        List<String> textList = Lists.newArrayList(Lists.transform(number,
                new Function<Long, String>() {
                    public String apply(Long input) {
                        return String.valueOf(input);
                    }
                }));
        String[] keyArr = textList.toArray(new String[number.size()]);
        jedisClient.rpush(combineKeyNameSpace(nameSpace, key), keyArr);
    }

    @Override
    public List<Long> lrangeLong(String key, long start, long end) {
        List<String> stringList = jedisClient.lrange(combineKeyNameSpace(nameSpace, key), start, end);
        return Lists.newArrayList(Lists.transform(stringList, new Function<String, Long>() {
            public Long apply(String element) {
                if (element != null) {
                    return NumberUtils.toLong(element, 0);
                }
                return null;
            }
        }));
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return jedisClient.lrange(combineKeyNameSpace(nameSpace, key), start, end);
    }

    @Override
    public Number getNumber(String key) {
        String text = jedisClient.get(combineKeyNameSpace(nameSpace, key));
        return ByteUtils.stringToNumber(text);
    }

    @Override
    public String get(String key) {
        return jedisClient.get(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public <T extends Number> void set(String key, T value) {
        jedisClient.set(combineKeyNameSpace(nameSpace, key), String.valueOf(value));
    }

    @Override
    public <T extends Number> void set(String key, T value, int seconds) {
        jedisClient.setex(combineKeyNameSpace(nameSpace, key), seconds, String.valueOf(value));
    }

    @Override
    public List<Number> getList(String key) {
        List<String> stringList = jedisClient.lrange(combineKeyNameSpace(nameSpace, key), 0, -1);
        if (stringList == null) {
            return new ArrayList<>(0);
        }
        return Lists.newArrayList(Lists.transform(stringList, new Function<String, Number>() {
            public Number apply(String element) {
                return ByteUtils.stringToNumber(element);
            }
        }));
    }

    @Override
    public <T extends Number> void setList(String key, List<T> list) {
        del(key);
        addList(key, list);
    }

    @Override
    public <T extends Number> void setList(String key, List<T> list, int seconds) {
        setList(key, list);
        expire(key, seconds);
    }

    @Override
    public <T extends Number> void addElement(String key, T element) {
        jedisClient.rpush(combineKeyNameSpace(nameSpace, key), ByteUtils.numberToString(element));
    }

    @Override
    public <T extends Number> void addList(String key, List<T> list) {
        List<String> stringList = Lists.newArrayList(Lists.transform(list,
                new Function<T, String>() {
                    public String apply(T element) {
                        return ByteUtils.numberToString(element);
                    }
                }));
        String[] keyArr = stringList.toArray(new String[stringList.size()]);
        jedisClient.rpush(combineKeyNameSpace(nameSpace, key), keyArr);
    }

    @Override
    public Map<String, String> mget(List<String> keys) {
        if (keys != null && keys.size() > 0) {
            String[] keyArr = keys.toArray(new String[keys.size()]);
            for (int index = 0; index < keys.size(); ++index) {
                String key = keys.get(index);
                keyArr[index] = combineKeyNameSpace(nameSpace, key);
            }

            List<String> values = multiKeyClient.mget(keyArr);
            Map<String, String> resultMap = MapUtils.createHashMap(keyArr.length);
            if (values != null && keys.size() == values.size()) {
                for (int i = 0; i < keys.size(); i++) {
                    String value = values.get(i);
                    if (StringUtils.isNotBlank(value)) {
                        resultMap.put(keys.get(i), value);
                    }
                }
            }
            return resultMap;
        }
        return MapUtils.createEmptyMap();
    }

    @Override
    public <T> void mset(Map<String, T> values, Class<T> clazz) {
        mset(values, clazz, -1);
    }

    @Override
    public <T> void mset(Map<String, T> values, Class<T> clazz, int expire) {
        if (values != null && values.size() > 0) {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, String> strMap = MapUtils.createHashMap(values.size());
                for (Map.Entry<String, T> e : values.entrySet()) {
                    String key = combineKeyNameSpace(nameSpace, e.getKey());
                    String valueStr = ByteUtils.objectToString(e.getValue(), clazz);
                    strMap.put(key, valueStr);
                }
                ((TedisClusterClient) multiKeyClient).mset(strMap, expire);
            }
        }
    }

    @Override
    public <T> Map<String, T> mget(List<String> keys, Class<T> clazz) {
        if (keys != null && keys.size() > 0) {
            ArrayList<String> combineKeys = new ArrayList<>(keys.size());
            for (int values = 0; values < keys.size(); ++values) {
                String key = keys.get(values);
                combineKeys.add(combineKeyNameSpace(nameSpace, key));
            }

            String[] keyArr = combineKeys.toArray(new String[combineKeys.size()]);
            List<String> values = multiKeyClient.mget(keyArr);
            Map<String, T> resultMap = MapUtils.createHashMap(keyArr.length);
            if (values != null && keys.size() == values.size()) {
                for (int i = 0; i < keys.size(); i++) {
                    String value = values.get(i);
                    if (StringUtils.isNotBlank(value)) {
                        resultMap.put(keys.get(i), ByteUtils.stringToObject(value, clazz));
                    }
                }
            }
            return resultMap;
        }
        return MapUtils.createEmptyMap();
    }

    @Override
    public void mset(Map<String, String> values) {
        mset(values, -1);
    }

    @Override
    public void mset(Map<String, String> values, int expire) {
        if (values != null && values.size() > 0) {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, String> newValues = MapUtils.createHashMap(values.size());
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    String key = combineKeyNameSpace(nameSpace, entry.getKey());
                    newValues.put(key, entry.getValue());
                }
                ((TedisClusterClient) multiKeyClient).mset(newValues, expire);
            }
        }
    }

    @Override
    public <T> void set(String key, T object, Class<T> clazz) {
        //传入null的语义:如果传入null值,那么不set,反而将这个key删除掉
        if (object != null) {
            String text = ByteUtils.objectToString(object, clazz);
            jedisClient.set(combineKeyNameSpace(nameSpace, key), text);
        } else {
            del(key);
        }
    }

    @Override
    public void set(String key, String value) {
        jedisClient.set(combineKeyNameSpace(nameSpace, key), value);
    }

    @Override
    public String getset(String key, String value) {
        return jedisClient.getSet(combineKeyNameSpace(nameSpace, key), value);
    }

    @Override
    public void set(String key, String value, int seconds) {
        jedisClient.setex(combineKeyNameSpace(nameSpace, key), seconds, value);
    }

    @Override
    public <T> void set(String key, T object, Class<T> clazz, int seconds) {
        jedisClient.setex(combineKeyNameSpace(nameSpace, key), seconds, ByteUtils.objectToString(object, clazz));
    }

    @Override
    public <T> List<T> getList(String key, Class<T> clazz) {
        throw new UnsupportedOperationException("getList(String key, Class<T> clazz)");
    }

    @Override
    public <T> List<T> getList(String key, long start, long end, Class<T> clazz) {
        throw new UnsupportedOperationException("getList(String key, long start, long end, Class<T> clazz)");
    }

    @Override
    public int rpush(String key, String value) {
        return jedisClient.rpush(combineKeyNameSpace(nameSpace, key), value).intValue();
    }

    @Override
    public <T> int rpush(String key, T value, Class<T> clazz) {
        return jedisClient.rpush(combineKeyNameSpace(nameSpace, key),
                new String[]{ByteUtils.objectToString(value, clazz)}).intValue();
    }

    @Override
    public int rpush(String key, String... value) {
        return jedisClient.rpush(combineKeyNameSpace(nameSpace, key), value).intValue();
    }

    @Override
    public int lpush(String key, String value) {
        return jedisClient.lpush(combineKeyNameSpace(nameSpace, key), value).intValue();
    }

    @Override
    public int lpush(String key, String... value) {
        return jedisClient.lpush(combineKeyNameSpace(nameSpace, key), value).intValue();
    }

    @Override
    public String lpop(String key) {
        return jedisClient.lpop(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public <T> T lpop(String key, Class<T> clazz) {
        String value = lpop(key);
        return ByteUtils.stringToObject(value, clazz);
    }

    @Override
    public String rpop(String key) {
        return jedisClient.rpop(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public String lpeek(String key) {
        return lindex(key, 0);
    }

    @Override
    public String lindex(String key, int index) {
        return jedisClient.lindex(combineKeyNameSpace(nameSpace, key), index);
    }

    @Override
    public String lset(String key, int index, String value) {
        return jedisClient.lset(combineKeyNameSpace(nameSpace, key), index, value);
    }

    @Override
    public Long getListLen(String key) {
        return jedisClient.llen(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public String ltrim(String key, int start, int end) {
        return jedisClient.ltrim(combineKeyNameSpace(nameSpace, key), start, end);
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return jedisClient.lrem(combineKeyNameSpace(nameSpace, key), count, value);
    }

    @Override
    public <T> void setList(String key, List<T> list, Class<T> clazz) {
        throw new UnsupportedOperationException("setList(String key, List<T> list, Class<T> clazz)");
    }

    @Override
    public <T> void setList(String key, List<T> list, Class<T> clazz, int seconds) {
        throw new UnsupportedOperationException("setList(String key, List<T> list, Class<T> clazz, int seconds)");
    }

    @Override
    public <T> void addElement(String key, T element, Class<T> clazz) {
        throw new UnsupportedOperationException("addElement(String key, T element, Class<T> clazz)");
    }

    @Override
    public <T> void addList(String key, List<T> list, Class<T> clazz) {
        throw new UnsupportedOperationException("addList(String key, List<T> list, Class<T> clazz)");
    }

    @Override
    public void sadd(String key, String... values) {
        jedisClient.sadd(combineKeyNameSpace(nameSpace, key), values);
    }

    @Override
    public void saddLong(String key, Long... numbers) {
        jedisClient.sadd(combineKeyNameSpace(nameSpace, key), ArrayUtils.longArrayToStringArray(numbers));
    }

    @Override
    public void saddLong(String key, Set<Long> numberSet) {
        jedisClient.sadd(combineKeyNameSpace(nameSpace, key), SetUtils.longSetToStringArray(numberSet));
    }

    @Override
    public void sremLong(String key, Long... numbers) {
        jedisClient.srem(combineKeyNameSpace(nameSpace, key), ArrayUtils.longArrayToStringArray(numbers));
    }

    @Override
    public void sremLong(String key, Set<Long> numberSet) {
        jedisClient.srem(combineKeyNameSpace(nameSpace, key), SetUtils.longSetToStringArray(numberSet));
    }

    @Override
    public Set<Long> smembersLong(String key) {
        return SetUtils.stringSetToLongSet(jedisClient.smembers(combineKeyNameSpace(nameSpace, key)));
    }

    @Override
    public Set<String> smembers(String key) {
        return jedisClient.smembers(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public boolean sisMemember(String key, Long number) {
        return jedisClient.sismember(combineKeyNameSpace(nameSpace, key), String.valueOf(number));
    }

    @Override
    public boolean sisMember(String key, String member) {
        return jedisClient.sismember(combineKeyNameSpace(nameSpace, key), member);
    }

    @Override
    public Map<String, Boolean> mSisMember(String key, List<String> members) {
        if (multiKeyClient instanceof TedisClusterClient) {
            return ((TedisClusterClient) multiKeyClient).mSisMember(combineKeyNameSpace(nameSpace, key), members);
        }
        return MapUtils.createEmptyMap();
    }

    @Override
    public Long scard(String key) {
        return jedisClient.scard(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public Long llen(String key) {
        return jedisClient.llen(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public List<Set<Long>> smembersLongSerial(Collection<String> keyCollection) {
        throw new UnsupportedOperationException("smembersLongSerial(Collection<String> keyCollection)");
    }

    @Override
    public <T> Long hset(String key, String field, T value, Class<T> clazz) {
        return jedisClient.hset(combineKeyNameSpace(nameSpace, key), field,
                ByteUtils.objectToString(value, clazz));
    }

    @Override
    public Long hset(String key, String field, String value) {
        return jedisClient.hset(combineKeyNameSpace(nameSpace, key), field, value);
    }

    @Override
    public <T> T hget(String key, String field, Class<T> clazz) {
        return ByteUtils.stringToObject(jedisClient.hget(combineKeyNameSpace(nameSpace, key), field), clazz);
    }

    @Override
    public String hget(String key, String field) {
        return jedisClient.hget(combineKeyNameSpace(nameSpace, key), field);
    }

    @Override
    public <T> Long hsetnx(String key, String field, T value, Class<T> clazz) {
        return jedisClient.hsetnx(combineKeyNameSpace(nameSpace, key), field, ByteUtils.objectToString(value, clazz));
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return jedisClient.hsetnx(combineKeyNameSpace(nameSpace, key), field, value);
    }

    @Override
    public <T> String hmset(String key, Map<String, T> hash, Class<T> clazz) {
        return jedisClient.hmset(combineKeyNameSpace(nameSpace, key),
                Maps.newHashMap(Maps.transformValues(hash, generateObjectToStringFunction(clazz))));
    }

    private <T> Function<T, String> generateObjectToStringFunction(final Class<T> clazz) {
        return new Function<T, String>() {
            public String apply(T value) {
                return ByteUtils.objectToString(value, clazz);
            }
        };
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return jedisClient.hmset(combineKeyNameSpace(nameSpace, key), hash);
    }

    private <T> void checkHashFieldMap(Map<String, T> fieldMap) {
        if (fieldMap == null || fieldMap.size() > MAX_HASH_HEY_FILED_THRESHOLD) {
            throw new IllegalArgumentException("fieldMap == null || " + (fieldMap == null ? 0 : fieldMap.size())
                    + " > " + MAX_HASH_HEY_FILED_THRESHOLD + ", fieldMap.size cannot over "
                    + MAX_HASH_HEY_FILED_THRESHOLD);
        }
    }

    private String timeCost(long startMills) {
        return (System.currentTimeMillis() - startMills) + "ms";
    }

    @Override
    public <T> String hsetAll(String key, Map<String, T> fieldMap, Class<T> clazz, int expireSeconds) throws IllegalArgumentException {
        checkHashFieldMap(fieldMap);
        long start = System.currentTimeMillis();
        try {
            if (jedisClient instanceof TedisClusterClient) {
                Map<String, String> combinedFieldMap = Maps.newHashMap(Maps.transformValues(fieldMap, generateObjectToStringFunction(clazz)));
                return ((TedisClusterClient) jedisClient).hsetAll(combineKeyNameSpace(nameSpace, key), combinedFieldMap, expireSeconds);
            }
        } finally {
            logger.info("CacheServiceRedisImpl.hsetAll(" + key + ", " + fieldMap.size() + ", " + clazz + ", " + expireSeconds + "): cost: " + timeCost(start));
        }
        return null;
    }

    @Override
    public String hsetAll(String key, Map<String, String> fieldMap, int expireSeconds) throws IllegalArgumentException {
        checkHashFieldMap(fieldMap);

        long start = System.currentTimeMillis();
        try {
            if (jedisClient instanceof TedisClusterClient) {
                return ((TedisClusterClient) jedisClient).hsetAll(combineKeyNameSpace(nameSpace, key), fieldMap, expireSeconds);
            }
        } finally {
            logger.info("CacheServiceRedisImpl.hsetAll(" + key + ", " + fieldMap.size() + ", " + expireSeconds + "): cost: " + timeCost(start));
        }
        return null;
    }

    private <T> Function<String, T> generateStringToObjectFunction(final Class<T> clazz) {
        return new Function<String, T>() {
            public T apply(String value) {
                return ByteUtils.stringToObject(value, clazz);
            }
        };
    }

    @Override
    public <T> List<T> hmget(String key, Class<T> clazz, String... fields) {
        return Lists.newArrayList(Lists.transform(
                jedisClient.hmget(combineKeyNameSpace(nameSpace, key), fields),
                generateStringToObjectFunction(clazz)));
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return jedisClient.hmget(combineKeyNameSpace(nameSpace, key), fields);
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return jedisClient.hincrBy(combineKeyNameSpace(nameSpace, key), field, value);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return jedisClient.hexists(combineKeyNameSpace(nameSpace, key), field);
    }

    @Override
    public Long hdel(String key, String... fields) {
        return jedisClient.hdel(combineKeyNameSpace(nameSpace, key), fields);
    }

    @Override
    public Long hdel(String key, Set<String> fields) {
        return jedisClient.hdel(combineKeyNameSpace(nameSpace, key), ArrayUtils.stringCollectionToStringArray(fields));
    }

    @Override
    public Long hlen(String key) {
        return jedisClient.hlen(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public Set<String> hkeys(String key) {
        return jedisClient.hkeys(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public <T> List<T> hvals(String key, Class<T> clazz) {
        List<String> listFromCache = jedisClient.hvals(combineKeyNameSpace(nameSpace, key));
        if (listFromCache == null) {
            return new ArrayList<>(0);
        }
        return Lists.newArrayList(Lists.transform(listFromCache, generateStringToObjectFunction(clazz)));
    }

    @Override
    public List<String> hvals(String key) {
        return jedisClient.hvals(combineKeyNameSpace(nameSpace, key));
    }

    @Override
    public <T> Map<String, T> hgetAll(String key, Class<T> clazz) {
        Map<String, String> mapFromCache = jedisClient.hgetAll(combineKeyNameSpace(nameSpace, key));
        if (mapFromCache == null) {
            return MapUtils.createEmptyMap();
        }
        return Maps.newHashMap(Maps.transformValues(mapFromCache, generateStringToObjectFunction(clazz)));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return jedisClient.hgetAll(combineKeyNameSpace(nameSpace, key));
    }

    private void checkHashKeyList(List<String> keyList) {
        if (keyList == null || keyList.size() > MAX_HASH_HEY_THRESHOLD) {
            throw new IllegalArgumentException("hash == null || " + (keyList == null ? 0 : keyList.size()) + " > " + MAX_HASH_HEY_THRESHOLD + ", keys cannot over " + MAX_HASH_HEY_THRESHOLD);
        }
    }

    @Override
    public <T> Map<String, Map<String, T>> mhgetAll(List<String> keyList, Class<T> clazz) throws IllegalArgumentException {
        checkHashKeyList(keyList);

        long start = System.currentTimeMillis();
        try {
            if (jedisClient instanceof TedisClusterClient) {
                List<String> combinedKeys = getCombineKeyList(keyList);
                Map<String, Map<String, String>> cacheMap = ((TedisClusterClient) jedisClient).mhgetAll(combinedKeys);
                if (cacheMap != null) {
                    Map<String, Map<String, T>> resultMap = new HashMap<String, Map<String, T>>((int) (cacheMap.size() / 0.75) + 1);
                    for (int i = 0; i < keyList.size(); ++i) {
                        String key = keyList.get(i);
                        Map<String, String> strFiledMap = cacheMap.get(combinedKeys.get(i));
                        Map<String, T> filedMap = Maps.newHashMap(Maps.transformValues(strFiledMap, generateStringToObjectFunction(clazz)));
                        resultMap.put(key, filedMap);
                    }
                    return resultMap;
                }
                return MapUtils.createEmptyMap();
            }
        } finally {
            logger.info("CacheServiceRedisImpl.mhgetAll(" + keyList + ", " + clazz + ") cost:" + timeCost(start));
        }
        return MapUtils.createEmptyMap();
    }

    @Override
    public Map<String, Map<String, String>> mhgetAll(List<String> keyList) throws IllegalArgumentException {
        checkHashKeyList(keyList);

        long start = System.currentTimeMillis();
        try {
            if (jedisClient instanceof TedisClusterClient) {
                List<String> combinedKeys = getCombineKeyList(keyList);
                Map<String, Map<String, String>> cacheMap = ((TedisClusterClient) jedisClient).mhgetAll(combinedKeys);
                if (cacheMap != null) {
                    Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>((int) (cacheMap.size() / 0.75) + 1);
                    for (int i = 0; i < keyList.size(); ++i) {
                        String key = keyList.get(i);
                        resultMap.put(key, cacheMap.get(combinedKeys.get(i)));
                    }
                    return resultMap;
                }
                return MapUtils.createEmptyMap();
            }
        } finally {
            logger.info("CacheServiceRedisImpl.mhgetAll(" + keyList + ") cost:" + timeCost(start));
        }
        return MapUtils.createEmptyMap();
    }

    private <T> void checkHashCacheMap(Map<String, Map<String, T>> cacheMap) {
        if (cacheMap == null || cacheMap.size() > MAX_HASH_HEY_THRESHOLD) {
            throw new IllegalArgumentException("cacheMap == null || " + (cacheMap == null ? 0 : cacheMap.size()) + " > " + MAX_HASH_HEY_THRESHOLD + ", cacheMap.size cannot over " + MAX_HASH_HEY_THRESHOLD);
        }
    }

    @Override
    public <T> Long mhsetAll(Map<String, Map<String, T>> cacheMap, Class<T> clazz, int expire) throws IllegalArgumentException {
        checkHashCacheMap(cacheMap);
        long start = System.currentTimeMillis();
        try {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, Map<String, String>> combinedCacheMap = new HashMap<String, Map<String, String>>((int) (cacheMap.size() / 0.75) + 1);
                for (Map.Entry<String, Map<String, T>> entry : cacheMap.entrySet()) {
                    String keyWithNS = combineKeyNameSpace(nameSpace, entry.getKey());
                    Map<String, T> fieldMap = entry.getValue();
                    checkHashFieldMap(fieldMap);
                    Map<String, String> filedMap = Maps.newHashMap(Maps.transformValues(fieldMap, generateObjectToStringFunction(clazz)));
                    combinedCacheMap.put(keyWithNS, filedMap);
                }
                return ((TedisClusterClient) multiKeyClient).mhsetAll(combinedCacheMap, expire);
            }
        } finally {
            logger.info("CacheServiceRedisImpl.mhsetAll(" + cacheMap.size() + ", " + clazz + ", " + expire + ") cost:" + timeCost(start));
        }
        return null;
    }

    @Override
    public Long mhsetAll(Map<String, Map<String, String>> cacheMap, int expire) throws IllegalArgumentException {
        checkHashCacheMap(cacheMap);
        long start = System.currentTimeMillis();
        try {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, Map<String, String>> combineCacheMap = new HashMap<String, Map<String, String>>((int) (cacheMap.size() / 0.75) + 1);
                for (Map.Entry<String, Map<String, String>> entry : cacheMap.entrySet()) {
                    String keyWithNS = combineKeyNameSpace(nameSpace, entry.getKey());
                    Map<String, String> fieldMap = entry.getValue();
                    checkHashFieldMap(fieldMap);
                    combineCacheMap.put(keyWithNS, fieldMap);
                }
                return ((TedisClusterClient) multiKeyClient).mhsetAll(combineCacheMap, expire);
            }
        } finally {
            logger.info("CacheServiceRedisImpl.mhsetAll(" + cacheMap.size() + ", " + expire + ") cost:" + timeCost(start));
        }
        return null;
    }

    @Override
    public <T> T get(String key, Class<T> clazz) {
        String value = jedisClient.get(combineKeyNameSpace(nameSpace, key));
        return StringUtils.isNotBlank(value) ? ByteUtils.stringToObject(value, clazz) : null;
    }
}
