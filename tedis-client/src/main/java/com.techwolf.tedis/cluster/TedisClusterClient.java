package com.techwolf.tedis.cluster;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.techwolf.TechwolfJedisConfig;
import redis.clients.util.JedisClusterCRC16;

import java.util.*;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public class TedisClusterClient extends TechwolfJedisCluster implements JedisCommands,
        MultiKeyJedisClusterCommands, JedisClusterScriptingCommands {

    private static final Logger logger = LoggerFactory.getLogger(TedisClusterClient.class);

    public TedisClusterClient(TechwolfJedisConfig config) {
        super(config);
    }

    @Override
    public List<String> mget(String... keys) {

        List<String> keyList = Lists.newArrayList(keys);
        ImmutableListMultimap<Integer, String> keyGroupBySlot = Multimaps.index(keyList,
                new Function<String, Integer>() {
                    public Integer apply(String key) {
                        return JedisClusterCRC16.getSlot(key);
                    }
                });
        if (keyGroupBySlot.size() < 2) {//同一个slot直接返回
            return super.mget(keys);
        }

        Map<String, String> tempMap = new HashMap<String, String>();
        for (Map.Entry<Integer, Collection<String>> entry : keyGroupBySlot.asMap().entrySet()) {
            Collection<String> keys4SameSlot = entry.getValue();
            try {
                if (keys4SameSlot != null && keys4SameSlot.size() > 0) {
                    List<String> tempKeyList = new ArrayList<String>(keys4SameSlot);
                    List<String> subResult = super.mget(tempKeyList.toArray(new String[tempKeyList.size()]));
                    for (int i = 0; i < tempKeyList.size(); i++) {
                        tempMap.put(tempKeyList.get(i), subResult.get(i));
                    }
                }
            } catch (JedisConnectionException e) {
                //通过slot获取连接
                TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);
                slotConnHandler.renewSlotCache();
                logger.error("TedisClusterClient mget String ...keys has error ", e);
            }
        }
        List<String> result = new ArrayList<String>(keys.length);
        for (String key : keys) {
            result.add(tempMap.get(key));
        }
        return result;
    }

    public Long mset(Map<String, String> keysValues, int expire) {
        //通过slot获取连接
        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);

        ImmutableListMultimap<Integer, Map.Entry<String, String>> entryGroupByEntryKey = Multimaps
                .index(keysValues.entrySet(),
                        new Function<Map.Entry<String, String>, Integer>() {
                            @Override
                            public Integer apply(Map.Entry<String, String> entry) {
                                return JedisClusterCRC16.getSlot(entry.getKey());
                            }
                        });

        long result = 0;
        for (Map.Entry<Integer, Collection<Map.Entry<String, String>>> temp : entryGroupByEntryKey.asMap().entrySet()) {
            Collection<Map.Entry<String, String>> entries4SameSlot = temp.getValue();
            if (entries4SameSlot == null || entries4SameSlot.isEmpty()) {
                continue;
            }
            Jedis jedis = null;
            try {
                jedis = slotConnHandler.getConnectionFromSlot(temp.getKey());
                Pipeline pipeline = jedis.pipelined();
                List<Response<String>> respList = new ArrayList<Response<String>>(entries4SameSlot.size());
                for (Map.Entry<String, String> entry : entries4SameSlot) {
                    if (entry.getValue() == null || entry.getKey() == null) {
                        continue;
                    }
                    if (expire > 0) {
                        respList.add(pipeline.setex(entry.getKey(), expire, entry.getValue()));
                    } else {
                        respList.add(pipeline.set(entry.getKey(), entry.getValue()));
                    }
                }
                pipeline.sync();
                for (int i = 0; i < respList.size(); ++i) {
                    if ("OK".equals(respList.get(i).get())) {
                        result += 1;
                    }
                }
            } catch (JedisConnectionException e) {
                slotConnHandler.renewSlotCache();
                logger.error("TedisClusterClient mset has error", e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        return result;
    }

    private TechwolfJedisSlotBasedConnectionHandler castConnectionHandlerForSlotBased(TechwolfJedisClusterConnectionHandler connectionHandler) {
        if (connectionHandler instanceof TechwolfJedisSlotBasedConnectionHandler) {
            return (TechwolfJedisSlotBasedConnectionHandler) connectionHandler;
        } else {
            throw new JedisClusterException("connectionHandler can not get connect by slot");
        }
    }

    public String hsetAll(String key, Map<String, String> fieldMap, int expireSeconds) {
        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);
        if (fieldMap == null || fieldMap.isEmpty()) {
            return null;
        }
        int slot = JedisClusterCRC16.getSlot(key);
        Jedis jedis = null;
        try {
            jedis = slotConnHandler.getConnectionFromSlot(slot);
            Pipeline pipeline = jedis.pipelined();
            pipeline.del(key);
            Response<String> hmsetResp = pipeline.hmset(key, fieldMap);
            pipeline.expire(key, expireSeconds);
            pipeline.sync();
            return hmsetResp.get();
        } catch (JedisConnectionException e) {
            slotConnHandler.renewSlotCache();
            logger.error("TedisClusterClient hsetAll has error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    public Map<String, Map<String, String>> mhgetAll(List<String> keyList) {
        final TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);
        ImmutableListMultimap<Integer, String> keyGroupByPool = Multimaps.index(keyList,
                new Function<String, Integer>() {
                    @Override
                    public Integer apply(String key) {
                        return JedisClusterCRC16.getSlot(key);
                    }
                });

        Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>((int) (keyList.size() / 0.75) + 1);
        for (Map.Entry<Integer, Collection<String>> entry : keyGroupByPool.asMap().entrySet()) {
            Collection<String> keys4SamePool = entry.getValue();
            if (keys4SamePool == null || keys4SamePool.isEmpty()) {
                continue;
            }
            Jedis jedis = null;
            try {
                jedis = slotConnHandler.getConnectionFromSlot(entry.getKey());
                Pipeline pipeline = jedis.pipelined();
                List<Response<Map<String, String>>> respList = new ArrayList<Response<Map<String, String>>>(keys4SamePool.size());
                for (String key : keys4SamePool) {
                    respList.add(pipeline.hgetAll(key));
                }
                pipeline.sync();

                int i = 0;
                for (String key : keys4SamePool) {
                    Response<Map<String, String>> resp = respList.get(i++);
                    if (resp != null) {
                        resultMap.put(key, resp.get());
                    } else {
                        resultMap.put(key, null);
                    }
                }
            } catch (JedisConnectionException e) {
                slotConnHandler.renewSlotCache();
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        return resultMap;
    }

    public Map<String, Boolean> mExists(List<String> keysList) {

        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);

        ImmutableListMultimap<Integer, String> keyGroupBySlot = Multimaps
                .index(keysList,
                        new Function<String, Integer>() {
                            @Override
                            public Integer apply(String key) {
                                return JedisClusterCRC16.getSlot(key);
                            }
                        });

        Map<String, Boolean> resultMap = new HashMap<String, Boolean>((int) (keysList.size() / 0.75 + 1));
        for (Map.Entry<Integer, Collection<String>> entry : keyGroupBySlot.asMap().entrySet()) {
            if (entry.getValue() == null || entry.getValue().isEmpty()) {
                continue;
            }

            Jedis jedis = null;
            try {
                jedis = slotConnHandler.getConnectionFromSlot(entry.getKey());
                Pipeline pipeline = jedis.pipelined();
                Map<String, Response<Boolean>> respMap = new HashMap<String, Response<Boolean>>((int) (entry.getValue().size() / 0.75) + 1);
                for (String key : entry.getValue()) {
                    if (key == null) {
                        continue;
                    }
                    respMap.put(key, pipeline.exists(key));
                }
                pipeline.sync();
                for (Map.Entry<String, Response<Boolean>> respEntry : respMap.entrySet()) {
                    String key = respEntry.getKey();
                    Response<Boolean> resp = respEntry.getValue();
                    resultMap.put(key, resp.get());
                }
            } catch (JedisConnectionException e) {
                slotConnHandler.renewSlotCache();
                logger.error("TedisClusterClient mExists has error", e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        return resultMap;
    }

    public Long expire(List<String> keyList, int seconds) {
        //通过slot获取连接
        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);

        ImmutableListMultimap<Integer, String> keyGroupBySlot = Multimaps
                .index(keyList,
                        new Function<String, Integer>() {
                            @Override
                            public Integer apply(String key) {
                                return JedisClusterCRC16.getSlot(key);
                            }
                        });

        long result = 0;
        for (Map.Entry<Integer, Collection<String>> temp : keyGroupBySlot.asMap().entrySet()) {
            Collection<String> key4SameSlot = temp.getValue();
            if (key4SameSlot == null || key4SameSlot.isEmpty()) {
                continue;
            }
            Jedis jedis = null;
            try {
                jedis = slotConnHandler.getConnectionFromSlot(temp.getKey());
                Pipeline pipeline = jedis.pipelined();
                List<Response<Long>> respList = new ArrayList<Response<Long>>(key4SameSlot.size());
                for (String key : key4SameSlot) {
                    if (key == null) {
                        continue;
                    }
                    if (seconds > 0) {
                        respList.add(pipeline.expire(key, seconds));
                    }
                }
                pipeline.sync();
                for (int i = 0; i < respList.size(); ++i) {
                    if (respList.get(i).get() != null && 1L == respList.get(i).get()) {
                        result += 1;
                    }
                }
            } catch (JedisConnectionException e) {
                slotConnHandler.renewSlotCache();
                logger.error("TedisClusterClient expire has error", e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        return result;
    }

    private Jedis getJedisBySlot(int key) {
        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);
        return slotConnHandler.getConnectionFromSlot(key);
    }

    private Jedis getJedisByKey(String key) {
        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);
        return slotConnHandler.getConnectionFromSlot(JedisClusterCRC16.getSlot(key));
    }

    public Long mhsetAll(Map<String, Map<String, String>> cacheMap, int expire) {
        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);
        ImmutableListMultimap<Integer, Map.Entry<String, Map<String, String>>> entryGroupByPool = Multimaps.index(cacheMap.entrySet(),
                new Function<Map.Entry<String, Map<String, String>>, Integer>() {
                    @Override
                    public Integer apply(Map.Entry<String, Map<String, String>> entry) {
                        return JedisClusterCRC16.getSlot(entry.getKey());
                    }
                });

        Long result = 0L;
        for (Map.Entry<Integer, Collection<Map.Entry<String, Map<String, String>>>> entry : entryGroupByPool.asMap().entrySet()) {
            Collection<Map.Entry<String, Map<String, String>>> entries4SamePool = entry.getValue();
            if (entries4SamePool == null || entries4SamePool.isEmpty()) {
                continue;
            }
            Jedis jedis = null;
            try {
                jedis = slotConnHandler.getConnectionFromSlot(entry.getKey());
                Pipeline pipeline = jedis.pipelined();
                List<Response<String>> respList = new ArrayList<Response<String>>(entryGroupByPool.size());
                for (Map.Entry<String, Map<String, String>> hashEntry : entries4SamePool) {
                    String hashKey = hashEntry.getKey();
                    pipeline.del(hashKey);
                    respList.add(pipeline.hmset(hashKey, hashEntry.getValue()));
                    pipeline.expire(hashKey, expire);
                }
                pipeline.sync();

                for (int i = 0; i < respList.size(); ++i) {
                    result += ("OK".equals(respList.get(i).get()) ? 1 : 0);
                }
            } catch (JedisConnectionException e) {
                slotConnHandler.renewSlotCache();
                logger.error("TedisClusterClient mhsetAll has error", e);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        return result;
    }

    public Map<String, Boolean> mSisMember(String key, List<String> members) {
        TechwolfJedisSlotBasedConnectionHandler slotConnHandler = castConnectionHandlerForSlotBased(connectionHandler);
        int slot = JedisClusterCRC16.getSlot(key);
        Jedis jedis = null;
        try {
            jedis = slotConnHandler.getConnectionFromSlot(slot);
            Pipeline pipeline = jedis.pipelined();

            Map<String, Response<Boolean>> respMap = new HashMap<String, Response<Boolean>>((int) (members.size() / 0.75) + 1);
            for (int i = 0; i < members.size(); ++i) {
                String member = members.get(i);
                Response<Boolean> resp = pipeline.sismember(key, member);
                respMap.put(member, resp);
            }
            pipeline.sync();

            Map<String, Boolean> resultMap = new HashMap<String, Boolean>((int) (members.size() / 0.75) + 1);
            for (Map.Entry<String, Response<Boolean>> entry : respMap.entrySet()) {
                String member = entry.getKey();
                Response<Boolean> resp = entry.getValue();
                resultMap.put(member, resp.get());
            }

            return resultMap;
        } catch (JedisConnectionException e) {
            slotConnHandler.renewSlotCache();
            logger.error("TedisClusterClient mSisMember has error", e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new HashMap<String, Boolean>(0);
    }
}
