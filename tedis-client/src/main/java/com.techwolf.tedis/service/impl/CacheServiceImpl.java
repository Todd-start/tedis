package com.techwolf.tedis.service.impl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.techwolf.tedis.cluster.TedisClusterClient;
import com.techwolf.tedis.codec.*;
import com.techwolf.tedis.service.CacheService;
import com.techwolf.tedis.util.NameSpaceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisClusterScriptingCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.MultiKeyJedisClusterCommands;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public class CacheServiceImpl implements CacheService {

    private static final Logger logger = LoggerFactory.getLogger(CacheServiceImpl.class);

    private static final String DEFAULT_NAMESPACE = "boss";

    private String nameSpace = DEFAULT_NAMESPACE;

    private static final int MAX_HASH_HEY_FILED_THRESHOLD = 5000;

    private static final int MAX_HASH_HEY_THRESHOLD = 3000;

    public static final int TIMEOUT = 3000;

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

    private ExecutorService executorService = Executors.newFixedThreadPool(100);


    public Long del(String key) {
        return jedisClient.del(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public boolean exists(String key) {
        return jedisClient.exists(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public Map<String, Boolean> mExists(List<String> keyList) {
        if (multiKeyClient instanceof TedisClusterClient) {
            Map<String, Boolean> combineKeyResultMap = ((TedisClusterClient) multiKeyClient).mExists(getCombineKeyList(keyList));
            Map<String, Boolean> resultMap = new HashMap<String, Boolean>((int) (keyList.size() / 0.75) + 1);
            for (int i = 0; i < keyList.size(); ++i) {
                String key = keyList.get(i);
                String combineKey = NameSpaceUtil.combineKeyNameSpace(nameSpace, key);
                resultMap.put(key, combineKeyResultMap.get(combineKey));
            }
            return resultMap;
        }
        return new HashMap<String, Boolean>(0);
    }

    private List<String> getCombineKeyList(List<String> keyList) {
        List<String> combineKeys = new ArrayList<String>(keyList.size());
        for (int i = 0; i < keyList.size(); i++) {
            String key = keyList.get(i);
            combineKeys.add(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
        }
        return combineKeys;
    }

    public Long setnx(String key, String value) {
        return jedisClient.setnx(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), value);
    }

    public void expire(String key, int seconds) {
        jedisClient.expire(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), seconds);
    }

    public Long expire(List<String> keyList, int seconds) {
        if (multiKeyClient instanceof TedisClusterClient) {
            return ((TedisClusterClient) multiKeyClient).expire(getCombineKeyList(keyList), seconds);
        }
        return 0L;
    }

    public long ttl(String key) {
        return jedisClient.ttl(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public Long getLong(String key) {
        String value = get(key);
        return NumberUtils.toLong(value, 0);
    }

    public void setLong(String key, long value) {
        set(key, String.valueOf(value));
    }

    public void setLong(String key, long value, int seconds) {
        jedisClient.setex(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), seconds, String.valueOf(value));
    }

    public void zadd(String key, int score, String member) {
        zadd(key, score, member);
    }

    public void zadd(String key, Map<String, Double> scoreMemers) {
        jedisClient.zadd(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), scoreMemers);
    }

    public void zadd(String key, long score, String member) {
        jedisClient.zadd(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), score, member);
    }

    public int zrank(String key, String member) {
        return jedisClient.zrank(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), member).intValue();
    }

    public int zcard(String key) {
        return jedisClient.zcard(NameSpaceUtil.combineKeyNameSpace(nameSpace, key)).intValue();
    }

    public Double zscore(String key, String member) {
        return jedisClient.zscore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), member);
    }

    public int zremrangeByRank(String key, int start, int end) {
        return jedisClient.zremrangeByRank(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, end).intValue();
    }

    public int zrevrank(String key, String member) {
        return jedisClient.zrevrank(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), member).intValue();
    }

    public List<String> zrevrange(String key, int start, int stop) {
        return new ArrayList<String>(jedisClient.zrevrange(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, stop));
    }

    public List<String> zrange(String key, int start, int stop) {
        return new ArrayList<String>(jedisClient.zrange(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, stop));
    }

    public List<Tuple> zrevrangeWithScore(String key, int start, int stop) {
        return new ArrayList<Tuple>(jedisClient.zrevrangeWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, stop));
    }

    public List<Tuple> zrangeWithScore(String key, int start, int stop) {
        return new ArrayList<Tuple>(jedisClient.zrangeWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, stop));
    }

    public List<String> zrangeByScore(String key, double min, double max) {
        return new ArrayList<String>(jedisClient.zrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), min, max));
    }

    public List<String> zrangeByScore(String key, String min, String max) {
        return new ArrayList<String>(jedisClient.zrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), min, max));
    }

    public List<String> zrevrangeByScore(String key, double max, double min) {
        return new ArrayList<String>(jedisClient.zrevrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min));
    }

    public List<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return new ArrayList<String>(jedisClient.zrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), min, max));
    }

    public List<String> zrevrangeByScore(String key, String max, String min) {
        return new ArrayList<String>(jedisClient.zrevrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min));
    }

    public List<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return new ArrayList<String>(jedisClient.zrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), min, max, offset, count));
    }

    public List<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return new ArrayList<String>(jedisClient.zrevrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    public List<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return new ArrayList<Tuple>(jedisClient.zrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), min, max));
    }

    public List<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return new ArrayList<Tuple>(jedisClient.zrevrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min));
    }

    public List<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return new ArrayList<Tuple>(jedisClient.zrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    public List<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return new ArrayList<String>(jedisClient.zrevrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    public List<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return new ArrayList<Tuple>(jedisClient.zrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), min, max));
    }

    public List<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return new ArrayList<Tuple>(jedisClient.zrevrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min));
    }

    public List<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return new ArrayList<Tuple>(jedisClient.zrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), min, max, offset, count));
    }

    public List<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return new ArrayList<Tuple>(jedisClient.zrevrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    public List<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return new ArrayList<Tuple>(jedisClient.zrevrangeByScoreWithScores(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), max, min, offset, count));
    }

    public Long zremrangeByScore(String key, double start, double end) {
        return jedisClient.zremrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, end);
    }

    public Long zremrangeByScore(String key, String start, String end) {
        return jedisClient.zremrangeByScore(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, end);
    }

    public void zrem(String key, String member) {
        jedisClient.zrem(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), member);
    }

    public void zrem(String key, String... members) {
        jedisClient.zrem(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), members);
    }

    public double zincrby(String key, double score, String member) {
        return jedisClient.zincrby(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), score, member);
    }

    public Long increment(String key) {
        return jedisClient.incr(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public Long incrementBy(String key, long delta) {
        return jedisClient.incrBy(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), delta);
    }

    public Long decrement(String key) {
        return jedisClient.decr(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public Long decrementBy(String key, long decrement) {
        return jedisClient.decrBy(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), decrement);
    }

    public List<Long> getLongList(String key) {
        List<String> bytesList = jedisClient.lrange(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), 0, -1);
        if (bytesList == null) {
            return new ArrayList<Long>(0);
        }
        return Lists.newArrayList(Lists.transform(bytesList, new Function<String, Long>() {
            public Long apply(String s) {
                return NumberUtils.toLong(s, 0);
            }
        }));
    }

    public void setLongList(String key, List<Long> list) {

    }

    public void setLongList(String key, List<Long> list, int seconds) {

    }

    public void addLongElement(String key, long number) {
        jedisClient.rpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), String.valueOf(number));
    }

    public void addLongList(String key, List<Long> number) {
        List<String> textList = Lists.newArrayList(Lists.transform(number,
                new Function<Long, String>() {
                    public String apply(Long input) {
                        return String.valueOf(input);
                    }
                }));
        String[] keyArr = textList.toArray(new String[number.size()]);
        jedisClient.rpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), keyArr);
    }

    public List<Long> lrangeLong(String key, long start, long end) {
        List<String> stringList = jedisClient.lrange(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, end);
        return Lists.newArrayList(Lists.transform(stringList, new Function<String, Long>() {
            public Long apply(String element) {
                if (element != null) {
                    return NumberUtils.toLong(element, 0);
                }
                return null;
            }
        }));
    }

    public List<String> lrange(String key, long start, long end) {
        return jedisClient.lrange(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, end);
    }

    public Number getNumber(String key) {
        String text = jedisClient.get(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
        return ByteUtils.stringToNumber(text);
    }

    public String get(String key) {
        return jedisClient.get(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public <T extends Number> void set(String key, T value) {
        jedisClient.set(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), String.valueOf(value));
    }

    public <T extends Number> void set(String key, T value, int seconds) {
        jedisClient.setex(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), seconds, String.valueOf(value));
    }

    public List<Number> getList(String key) {
        List<String> stringList = jedisClient.lrange(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), 0, -1);
        if (stringList == null) {
            return new ArrayList<Number>(0);
        }
        return Lists.newArrayList(Lists.transform(stringList, new Function<String, Number>() {
            public Number apply(String element) {
                return ByteUtils.stringToNumber(element);
            }
        }));
    }

    public <T extends Number> void setList(String key, List<T> list) {
        del(key);
        addList(key, list);
    }

    public <T extends Number> void setList(String key, List<T> list, int seconds) {
        setList(key, list);
        expire(key, seconds);
    }

    public <T extends Number> void addElement(String key, T element) {
        jedisClient.rpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), ByteUtils.numberToString(element));
    }

    public <T extends Number> void addList(String key, List<T> list) {
        List<String> stringList = Lists.newArrayList(Lists.transform(list,
                new Function<T, String>() {
                    public String apply(T element) {
                        return ByteUtils.numberToString(element);
                    }
                }));
        String[] keyArr = stringList.toArray(new String[stringList.size()]);
        jedisClient.rpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), keyArr);
    }

    public Map<String, String> mget(List<String> keys) {
        if (keys != null && keys.size() > 0) {
            String[] keyArr = keys.toArray(new String[keys.size()]);
            for (int index = 0; index < keys.size(); ++index) {
                String key = keys.get(index);
                keyArr[index] = NameSpaceUtil.combineKeyNameSpace(nameSpace, key);
            }
            List<String> values = multiKeyClient.mget(keyArr);
            Map<String, String> resultMap = new HashMap<String, String>((int) (keys.size() / 0.75) + 1);
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
        return new HashMap<String, String>(0);
    }

    public <T> void mset(Map<String, T> values, Class<T> clazz) {
        mset(values, clazz, -1);
    }

    public <T> void mset(Map<String, T> values, Class<T> clazz, int expire) {
        if (values != null && values.size() > 0) {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, String> strMap = new HashMap<String, String>((int) (values.size() / 0.75) + 1);
                for (Map.Entry<String, T> e : values.entrySet()) {
                    String key = NameSpaceUtil.combineKeyNameSpace(nameSpace, e.getKey());
                    String valueStr = ByteUtils.objectToString(e.getValue(), clazz);
                    strMap.put(key, valueStr);
                }
                ((TedisClusterClient) multiKeyClient).mset(strMap, expire);
            }
        }
    }

    public <T> Map<String, T> mget(List<String> keys, Class<T> clazz) {
        if (keys != null && keys.size() > 0) {
            ArrayList combineKeys = new ArrayList(keys.size());
            for (int values = 0; values < keys.size(); ++values) {
                String key = keys.get(values);
                combineKeys.add(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
            }
            String[] keyArr = (String[]) combineKeys.toArray(new String[combineKeys.size()]);
            List<String> values = multiKeyClient.mget(keyArr);
            Map<String, T> resultMap = new HashMap<String, T>((int) (keys.size() / 0.75) + 1);
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
        return new HashMap<String, T>(0);
    }

    public void mset(Map<String, String> values) {
        mset(values, -1);
    }

    public void mset(Map<String, String> values, int expire) {
        if (values != null && values.size() > 0) {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, String> newValues = new HashMap<String, String>((int) (values.size() / 0.75) + 1);
                for (Map.Entry<String, String> entry : values.entrySet()) {
                    String key = NameSpaceUtil.combineKeyNameSpace(nameSpace, entry.getKey());
                    newValues.put(key, entry.getValue());
                }
                ((TedisClusterClient) multiKeyClient).mset(newValues, expire);
            }
        }
    }

    public <T> void msetConcurrent(final Map<String, T> values, final Class<T> clazz) {
        if (values == null || values.isEmpty()) {
            return;
        }
        final CountDownLatch countDown = new CountDownLatch(values.size());
        for (final String key : values.keySet()) {
            //遍历批量任务的key,提交到线程池执行
            executorService.execute(new Runnable() {

                @Override
                public void run() {
                    //向redis发送请求,内部匿名类调用外部类的成员方法
                    try {
                        set(key, values.get(key), clazz);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    } finally {
                        countDown.countDown();//倒数减一,当到0时,主线程将会继续执行
                    }
                }
            });
        }
        try {
            countDown.await(TIMEOUT, TimeUnit.MILLISECONDS);//如果超过500ms不响应,主线程继续向西,避免无限阻塞
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public <T> T getBySerialStrategy(String key, Class<T> clazz, SerializableStrategy strategy) {
        return null;
    }

    public <T> void set(String key, T object, Class<T> clazz) {
        //传入null的语义:如果传入null值,那么不set,反而将这个key删除掉
        if (object != null) {
            String text = ByteUtils.objectToString(object, clazz);
            jedisClient.set(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), text);
        } else {
            del(key);
        }
    }

    public void set(String key, String value) {
        jedisClient.set(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), value);
    }

    public String getset(String key, String value) {
        return jedisClient.getSet(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), value);
    }

    public void set(String key, String value, int seconds) {
        jedisClient.setex(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), seconds, value);
    }

    public <T> void setBySerialStrategy(String key, T object, Class<T> clazz, SerializableStrategy strategy) {
        //传入null的语义:如果传入null值,那么不set,反而将这个key删除掉
        if (object != null) {
            byte[] bytes = ByteUtils.objectToByteArrayByStrategy(object, clazz, strategy);
            String text = ByteUtils.byteArrayToString(bytes, ByteUtils.CHARSET_FOR_ENCODING_OBJECT);
            jedisClient.set(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), text);
        } else {
            del(key);
        }
    }

    public <T> void set(String key, T object, Class<T> clazz, int seconds) {
        jedisClient.setex(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), seconds, ByteUtils.objectToString(object, clazz));
    }

    public <T> List<T> getList(String key, Class<T> clazz) {
        return null;
    }

    public <T> List<T> getList(String key, long start, long end, Class<T> clazz) {
        return null;
    }

    public int rpush(String key, String value) {
        return jedisClient.rpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), new String[]{value}).intValue();
    }

    public <T> int rpush(String key, T value, Class<T> clazz) {
        return jedisClient.rpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key),
                new String[]{ByteUtils.objectToString(value, clazz)}).intValue();
    }

    public int rpush(String key, String... value) {
        return jedisClient.rpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), value).intValue();
    }

    public int lpush(String key, String value) {
        return jedisClient.lpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), new String[]{value}).intValue();
    }

    public int lpush(String key, String... value) {
        return jedisClient.lpush(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), value).intValue();
    }

    public String lpop(String key) {
        return jedisClient.lpop(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public <T> T lpop(String key, Class<T> clazz) {
        String value = lpop(key);
        return ByteUtils.stringToObject(value, clazz);
    }

    public String rpop(String key) {
        return jedisClient.rpop(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public String lpeek(String key) {
        return lindex(key, 0);
    }

    public String lindex(String key, int index) {
        return jedisClient.lindex(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), index);
    }

    public String lset(String key, int index, String value) {
        return jedisClient.lset(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), index, value);
    }

    public Long getListLen(String key) {
        return jedisClient.llen(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public String ltrim(String key, int start, int end) {
        return jedisClient.ltrim(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), start, end);
    }

    public Long lrem(String key, long count, String value) {
        return jedisClient.lrem(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), count, value);
    }

    public <T> void setList(String key, List<T> list, Class<T> clazz) {

    }

    public <T> void setList(String key, List<T> list, Class<T> clazz, int seconds) {

    }

    public <T> void addElement(String key, T element, Class<T> clazz) {

    }

    public <T> void addList(String key, List<T> list, Class<T> clazz) {

    }

    public void sadd(String key, String... values) {
        jedisClient.sadd(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), values);
    }

    public void saddLong(String key, Long... numbers) {
        jedisClient.sadd(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), ArrayUtils.longArrayToStringArray(numbers));
    }

    public void saddLong(String key, Set<Long> numberSet) {
        jedisClient.sadd(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), SetUtils.longSetToStringArray(numberSet));
    }

    public void sremLong(String key, Long... numbers) {
        jedisClient.srem(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), ArrayUtils.longArrayToStringArray(numbers));
    }

    public void sremLong(String key, Set<Long> numberSet) {
        jedisClient.srem(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), SetUtils.longSetToStringArray(numberSet));
    }

    public Set<Long> smembersLong(String key) {
        return SetUtils.stringSetToLongSet(jedisClient.smembers(NameSpaceUtil.combineKeyNameSpace(nameSpace, key)));
    }

    public Set<String> smembers(String key) {
        return jedisClient.smembers(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public boolean sisMemember(String key, Long number) {
        return jedisClient.sismember(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), String.valueOf(number));
    }

    public boolean sisMember(String key, String member) {
        return jedisClient.sismember(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), member);
    }

    public Map<String, Boolean> mSisMember(String key, List<String> members) {
        if (multiKeyClient instanceof TedisClusterClient) {
            return ((TedisClusterClient) multiKeyClient).mSisMember(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), members);
        }
        return new HashMap<String, Boolean>(0);
    }

    public Long scard(String key) {
        return jedisClient.scard(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public Map getMap(String key) {
        MapWrapper mapWrapper = this.get(key, MapWrapper.class);
        if (mapWrapper != null) {
            return mapWrapper.getMap();
        }
        return null;
    }

    public void setMap(String key, Map map) {
        MapWrapper mapWrapper = new MapWrapper();
        mapWrapper.setMap(map);
        this.set(key, mapWrapper, MapWrapper.class);
    }

    public Long llen(String key) {
        return jedisClient.llen(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public List<Set<Long>> smembersLongConcurrent(Collection<String> keyCollection) {
        return null;
    }

    public List<Set<Long>> smembersLongSerial(Collection<String> keyCollection) {
        return null;
    }

    public <T> Long hset(String key, String field, T value, Class<T> clazz) {
        return jedisClient.hset(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field,
                ByteUtils.objectToString(value, clazz));
    }

    public Long hset(String key, String field, String value) {
        return jedisClient.hset(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field, value);
    }

    public <T> T hget(String key, String field, Class<T> clazz) {
        return ByteUtils.stringToObject(jedisClient.hget(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field),
                clazz);
    }

    public String hget(String key, String field) {
        return jedisClient.hget(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field);
    }

    public <T> Long hsetnx(String key, String field, T value, Class<T> clazz) {
        return jedisClient.hsetnx(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field,
                ByteUtils.objectToString(value, clazz));
    }

    public Long hsetnx(String key, String field, String value) {
        return jedisClient.hsetnx(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field,
                value);
    }

    public <T> String hmset(String key, Map<String, T> hash, Class<T> clazz) {
        return jedisClient.hmset(NameSpaceUtil.combineKeyNameSpace(nameSpace, key),
                Maps.newHashMap(Maps.transformValues(hash, generateObjectToStringFunction(clazz))));
    }

    private <T> Function<T, String> generateObjectToStringFunction(final Class<T> clazz) {
        return new Function<T, String>() {
            public String apply(T value) {
                return ByteUtils.objectToString(value, clazz);
            }
        };
    }

    public String hmset(String key, Map<String, String> hash) {
        return jedisClient.hmset(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), hash);
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

    public <T> String hsetAll(String key, Map<String, T> fieldMap, Class<T> clazz, int expireSeconds) throws IllegalArgumentException {
        checkHashFieldMap(fieldMap);
        long start = System.currentTimeMillis();
        try {
            if (jedisClient instanceof TedisClusterClient) {
                Map<String, String> combinedFieldMap = Maps.newHashMap(Maps.transformValues(fieldMap, generateObjectToStringFunction(clazz)));
                return ((TedisClusterClient) jedisClient).hsetAll(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), combinedFieldMap, expireSeconds);
            }
        } finally {
            logger.info("CacheServiceRedisImpl.hsetAll(" + key + ", " + fieldMap.size() + ", " + clazz + ", " + expireSeconds + "): cost: " + timeCost(start));
        }
        return null;
    }

    public String hsetAll(String key, Map<String, String> fieldMap, int expireSeconds) throws IllegalArgumentException {
        checkHashFieldMap(fieldMap);

        long start = System.currentTimeMillis();
        try {
            if (jedisClient instanceof TedisClusterClient) {
                return ((TedisClusterClient) jedisClient).hsetAll(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), fieldMap, expireSeconds);
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

    public <T> List<T> hmget(String key, Class<T> clazz, String... fields) {
        return Lists.newArrayList(Lists.transform(
                jedisClient.hmget(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), fields),
                generateStringToObjectFunction(clazz)));
    }

    public List<String> hmget(String key, String... fields) {
        return jedisClient.hmget(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), fields);
    }

    public Long hincrBy(String key, String field, long value) {
        return jedisClient.hincrBy(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field, value);
    }

    public Boolean hexists(String key, String field) {
        return jedisClient.hexists(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), field);
    }

    public Long hdel(String key, String... fields) {
        return jedisClient.hdel(NameSpaceUtil.combineKeyNameSpace(nameSpace, key), fields);
    }

    public Long hdel(String key, Set<String> fields) {
        return jedisClient.hdel(NameSpaceUtil.combineKeyNameSpace(nameSpace, key),
                ArrayUtils.stringCollectionToStringArray(fields));
    }

    public Long hlen(String key) {
        return jedisClient.hlen(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public Set<String> hkeys(String key) {
        return jedisClient.hkeys(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public <T> List<T> hvals(String key, Class<T> clazz) {
        List<String> listFromCache = jedisClient.hvals(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
        if (listFromCache == null) {
            return new ArrayList<T>(0);
        }
        return Lists.newArrayList(Lists.transform(listFromCache, generateStringToObjectFunction(clazz)));
    }

    public List<String> hvals(String key) {
        return jedisClient.hvals(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    public <T> Map<String, T> hgetAll(String key, Class<T> clazz) {
        Map<String, String> mapFromCache = jedisClient.hgetAll(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
        if (mapFromCache == null) {
            return new HashMap<String, T>(0);
        }
        return Maps.newHashMap(Maps.transformValues(mapFromCache, generateStringToObjectFunction(clazz)));
    }

    public Map<String, String> hgetAll(String key) {
        return jedisClient.hgetAll(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
    }

    private void checkHashKeyList(List<String> keyList) {
        if (keyList == null || keyList.size() > MAX_HASH_HEY_THRESHOLD) {
            throw new IllegalArgumentException("hash == null || " + (keyList == null ? 0 : keyList.size()) + " > " + MAX_HASH_HEY_THRESHOLD + ", keys cannot over " + MAX_HASH_HEY_THRESHOLD);
        }
    }

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
                return new HashMap<String, Map<String, T>>(0);
            }
        } finally {
            logger.info("CacheServiceRedisImpl.mhgetAll(" + keyList + ", " + clazz + ") cost:" + timeCost(start));
        }
        return null;
    }

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
                return new HashMap<String, Map<String, String>>(0);
            }
        } finally {
            logger.info("CacheServiceRedisImpl.mhgetAll(" + keyList + ") cost:" + timeCost(start));
        }
        return null;
    }

    private <T> void checkHashCacheMap(Map<String, Map<String, T>> cacheMap) {
        if (cacheMap == null || cacheMap.size() > MAX_HASH_HEY_THRESHOLD) {
            throw new IllegalArgumentException("cacheMap == null || " + (cacheMap == null ? 0 : cacheMap.size()) + " > " + MAX_HASH_HEY_THRESHOLD + ", cacheMap.size cannot over " + MAX_HASH_HEY_THRESHOLD);
        }
    }

    public <T> Long mhsetAll(Map<String, Map<String, T>> cacheMap, Class<T> clazz, int expire) throws IllegalArgumentException {
        checkHashCacheMap(cacheMap);
        long start = System.currentTimeMillis();
        try {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, Map<String, String>> combinedCacheMap = new HashMap<String, Map<String, String>>((int) (cacheMap.size() / 0.75) + 1);
                for (Map.Entry<String, Map<String, T>> entry : cacheMap.entrySet()) {
                    String keyWithNS = NameSpaceUtil.combineKeyNameSpace(nameSpace, entry.getKey());
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

    public Long mhsetAll(Map<String, Map<String, String>> cacheMap, int expire) throws IllegalArgumentException {
        checkHashCacheMap(cacheMap);
        long start = System.currentTimeMillis();
        try {
            if (multiKeyClient instanceof TedisClusterClient) {
                Map<String, Map<String, String>> combineCacheMap = new HashMap<String, Map<String, String>>((int) (cacheMap.size() / 0.75) + 1);
                for (Map.Entry<String, Map<String, String>> entry : cacheMap.entrySet()) {
                    String keyWithNS = NameSpaceUtil.combineKeyNameSpace(nameSpace, entry.getKey());
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

    public List<Set<String>> hmfieldsConcurrent(Collection<String> keys) {
        return null;
    }


    public <T> T get(String key, Class<T> clazz) {
        String value = jedisClient.get(NameSpaceUtil.combineKeyNameSpace(nameSpace, key));
        return StringUtils.isNotBlank(value) ? ByteUtils.stringToObject(value, clazz) : null;
    }

    public <T> Map<String, T> mgetConcurrent(Collection<String> key, Class<T> clazz) {
        return null;
    }
}
