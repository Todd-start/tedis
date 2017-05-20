import com.techwolf.tedis.cluster.TedisClusterClient;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.techwolf.TechwolfJedisConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public class TedisClusterClientTest {

    TedisClusterClient tedisClusterClient;

    @Before
    public void before() {
        HostAndPort hostAndPort = new HostAndPort("192.168.1.18", 8000);
        TechwolfJedisConfig techwolfJedisConfig = new TechwolfJedisConfig();
        techwolfJedisConfig.setHostAndPort(hostAndPort);
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(10);
        genericObjectPoolConfig.setMaxTotal(20);
        genericObjectPoolConfig.setMaxWaitMillis(-1);
        genericObjectPoolConfig.setMinIdle(5);
        techwolfJedisConfig.setPoolConfig(genericObjectPoolConfig);
        tedisClusterClient = new TedisClusterClient(techwolfJedisConfig);
    }

    @After
    public void after() {
        Map<String, JedisPool> map = tedisClusterClient.getClusterNodes();
        for (JedisPool jedisPool : map.values()) {
            try {
                jedisPool.getResource().flushAll();
            } catch (Exception e) {

            } finally {
                jedisPool.close();
            }
        }
    }

    @Test
    public void testConn() {
        Assert.assertNotNull(tedisClusterClient.getClusterNodes());
    }

    @Test
    public void testSet() {
        Assert.assertEquals("set", "OK", tedisClusterClient.set("test1", "test1"));
    }

    @Test
    public void testGet() {
        testSet();
        Assert.assertEquals("get", "test1", tedisClusterClient.get("test1"));
    }

    @Test
    public void testDel() {
        testSet();
        Assert.assertEquals("del", "1", tedisClusterClient.del("test1").toString());
    }

    @Test
    public void testMget() {
        tedisClusterClient.set("test1", "test1");
        tedisClusterClient.set("test2", "test2");
        tedisClusterClient.set("test3", "test3");
        tedisClusterClient.set("test4", "test4");
        List<String> result = tedisClusterClient.mget(new String[]{"test2", "test1", "test4", "test3", "test1"});
        Assert.assertEquals("mget", result.size(), 5);
    }

    @Test
    public void testMsetExpire() {
        Map<String, String> keyValues = new HashMap<String, String>(4);
        keyValues.put("test1", "test1");
        keyValues.put("test2", "test2");
        keyValues.put("test3", "test3");
        keyValues.put("test4", "test4");
        tedisClusterClient.mset(keyValues, 60);
        Assert.assertTrue("testMsetExpire", tedisClusterClient.ttl("test1") > 0);
        Assert.assertTrue("testMsetExpire", tedisClusterClient.ttl("test2") > 0);
        Assert.assertTrue("testMsetExpire", tedisClusterClient.ttl("test3") > 0);
        Assert.assertTrue("testMsetExpire", tedisClusterClient.ttl("test4") > 0);
    }

    @Test
    public void testMexists() {
        tedisClusterClient.set("test1", "test1");
        tedisClusterClient.set("test2", "test2");
        tedisClusterClient.set("test3", "test3");
        tedisClusterClient.set("test4", "test4");
        List<String> keys = new ArrayList<String>(6);
        keys.add("test1");
        keys.add("miss1");
        keys.add("test2");
        keys.add("miss2");
        keys.add("test3");
        keys.add("test4");
        Map<String, Boolean> map = tedisClusterClient.mExists(keys);
        Assert.assertEquals("testMexists", keys.size(), map.size());
        Assert.assertTrue("testMexists", map.get("test1"));
        Assert.assertTrue("testMexists", map.get("test2"));
        Assert.assertTrue("testMexists", map.get("test3"));
        Assert.assertTrue("testMexists", map.get("test4"));
        Assert.assertTrue("testMexists", !map.get("miss1"));
        Assert.assertTrue("testMexists", !map.get("miss2"));
    }

    @Test
    public void testExpireList() {
        tedisClusterClient.set("test1", "test1");
        tedisClusterClient.set("test2", "test2");
        tedisClusterClient.set("test3", "test3");
        tedisClusterClient.set("test4", "test4");
        List<String> keys = new ArrayList<String>(6);
        keys.add("test1");
        keys.add("miss1");
        keys.add("test2");
        keys.add("miss2");
        keys.add("test3");
        keys.add("test4");
        long count = tedisClusterClient.expire(keys, 60);
        Assert.assertEquals("testExpireList", 4, count);
        Assert.assertTrue("testExpireList", tedisClusterClient.ttl("test1") > 0);
        Assert.assertTrue("testExpireList", tedisClusterClient.ttl("test2") > 0);
        Assert.assertTrue("testExpireList", tedisClusterClient.ttl("test3") > 0);
        Assert.assertTrue("testExpireList", tedisClusterClient.ttl("test4") > 0);
    }

    @Test
    public void testHsetAll() {
        Map<String, String> map = new HashMap<String, String>(4);
        map.put("f1", "11");
        map.put("f2", "22");
        map.put("f3", "33");
        map.put("f4", "44");
        String result = tedisClusterClient.hsetAll("test1", map, 60);
        Assert.assertTrue("testHsetAll", tedisClusterClient.ttl("test1") > 0);
        Assert.assertEquals("testHsetAll", tedisClusterClient.hget("test1", "f1"), "11");
        Assert.assertEquals("testHsetAll", tedisClusterClient.hget("test1", "f2"), "22");
        Assert.assertEquals("testHsetAll", tedisClusterClient.hget("test1", "f3"), "33");
        Assert.assertEquals("testHsetAll", tedisClusterClient.hget("test1", "f4"), "44");
    }

    @Test
    public void testmhgetAll() {
        Map<String, String> map = new HashMap<String, String>(4);
        map.put("f1", "11");
        map.put("f2", "22");
        map.put("f3", "33");
        map.put("f4", "44");
        map.put("f5", "55");
        tedisClusterClient.hsetAll("test1", map, 60);
        tedisClusterClient.hsetAll("test2", map, 60);
        tedisClusterClient.hsetAll("test3", map, 60);
        tedisClusterClient.hsetAll("test4", map, 60);
        List<String> keysList = new ArrayList<String>();
        keysList.add("test1");
        keysList.add("test2");
        keysList.add("test3");
        keysList.add("test4");
        Map<String, Map<String, String>> mapMap = tedisClusterClient.mhgetAll(keysList);
        Assert.assertEquals("testmhgetAll", mapMap.size(), 4);
        Assert.assertEquals("testHsetAll", mapMap.get("test1").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test1").get("f1"), "11");
        Assert.assertEquals("testHsetAll", mapMap.get("test2").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test2").get("f2"), "22");
        Assert.assertEquals("testHsetAll", mapMap.get("test3").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test3").get("f3"), "33");
        Assert.assertEquals("testHsetAll", mapMap.get("test4").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test4").get("f4"), "44");
    }

    @Test
    public void mhsetAll() {
        Map<String, String> map = new HashMap<String, String>(4);
        map.put("f1", "11");
        map.put("f2", "22");
        map.put("f3", "33");
        map.put("f4", "44");
        map.put("f5", "55");
        Map<String, Map<String, String>> cacheMap = new HashMap<String, Map<String, String>>();
        cacheMap.put("test1", map);
        cacheMap.put("test2", map);
        cacheMap.put("test3", map);
        cacheMap.put("test4", map);
        tedisClusterClient.mhsetAll(cacheMap, 60);
        List<String> keysList = new ArrayList<String>();
        keysList.add("test1");
        keysList.add("test2");
        keysList.add("test3");
        keysList.add("test4");
        Map<String, Map<String, String>> mapMap = tedisClusterClient.mhgetAll(keysList);
        Assert.assertEquals("testmhgetAll", mapMap.size(), 4);
        Assert.assertEquals("testHsetAll", mapMap.get("test1").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test1").get("f1"), "11");
        Assert.assertEquals("testHsetAll", mapMap.get("test2").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test2").get("f2"), "22");
        Assert.assertEquals("testHsetAll", mapMap.get("test3").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test3").get("f3"), "33");
        Assert.assertEquals("testHsetAll", mapMap.get("test4").size(), 5);
        Assert.assertEquals("testHsetAll", mapMap.get("test4").get("f4"), "44");
    }

    @Test
    public void mSisMember() {
        List<String> member = new ArrayList<String>(3);
        member.add("11");
        member.add("aa");
        member.add("ccdao");
        member.add("111");
        tedisClusterClient.sadd("set1", new String[]{"11", "aa", "ccdao"});
        Map<String, Boolean> map = tedisClusterClient.mSisMember("set1", member);
        Assert.assertEquals("testmhgetAll", map.size(), 4);
        Assert.assertTrue("testmhgetAll", !map.get("111"));
        Assert.assertTrue("testmhgetAll", map.get("aa"));
    }

    /**
     * 1) "GEE_1499789"
     * 2) "GEE_1546914"
     * 3) "GEE_158653"
     * 4) "GEE_1662217"
     * 5) "GEE_1673256"
     * 6) "GEE_1676524"
     * 7) "GEE_170834"
     * 8) "GEE_189508"
     * 9) "GEE_217768"
     * 10) "GEE_240404"
     */
    @Test
    public void testASK() {
        String key = "GEE_1499789";//slot:16145 node:192.168.1.18:8000
        Object value = null;
        for (int i = 0; i < 100000; ++i) {
            value = tedisClusterClient.hgetAll(key);
            System.out.println(value);
        }
        Assert.assertNotNull("get", value);
    }
}
