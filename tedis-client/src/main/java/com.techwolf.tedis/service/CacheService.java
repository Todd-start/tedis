package com.techwolf.tedis.service;

import redis.clients.jedis.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhaoyalong on 17-3-25.
 * 实现业务需求的cache方法对外暴露此接口，不直接操作redis相关.
 */
public interface CacheService {

    /**
     * 针对key的通用操作
     */

    /**
     * 删除key所对应的key value对
     *
     * @param key key
     */
    Long del(String key);

    /**
     * 检查key，value对是否存在
     *
     * @param key key
     * @return 是否存在标志位，存在为true，不存在为false
     */
    boolean exists(String key);

    /**
     * 检查key，value对是否存在
     *
     * @param keyList keyList
     * @return 是否存在标志位映射，存在为true，不存在为false
     */
    Map<String, Boolean> mExists(List<String> keyList);

    Long setnx(String key, String value);

    /**
     * 设定过期时间，单位为秒，非重新set指令(push, add)不能改变过期时间
     *
     * @param key     key
     * @param seconds 过期秒数
     */
    void expire(String key, int seconds);

    /**
     * 设定过期时间，单位为秒，非重新set指令(push, add)不能改变过期时间
     *
     * @param keyList key
     * @param seconds 过期秒数
     */
    Long expire(List<String> keyList, int seconds);

    /**
     * 查询剩余过期时间，单位为秒，-1为存在或者不过期
     *
     * @param key key
     * @return 剩余过期时间(time to live) 时间为秒
     */
    long ttl(String key);

    /**
     * 获取数值类型value
     *
     * @param key key
     * @return long类型value
     */
    Long getLong(String key);

    /**
     * 设置数值类型value,默认永远不过期
     *
     * @param key   key
     * @param value long类型value
     */
    void setLong(String key, long value);

    /**
     * 设置数值类型value,与过期时间,
     *
     * @param key     key
     * @param value   long类型value
     * @param seconds 过期时间
     */
    void setLong(String key, long value, int seconds);

    void zadd(String key, int score, String member);

    void zadd(String key, Map<String, Double> scoreMemers);

    void zadd(String key, long score, String member);

    int zrank(String key, String member);

    int zcard(String key);

    Double zscore(String key, String member);

    int zremrangeByRank(String key, int start, int end);

    int zrevrank(String key, String member);

    List<String> zrevrange(String key, int start, int stop);

    List<String> zrange(String key, int start, int stop);

    List<Tuple> zrevrangeWithScore(String key, int start, int stop);

    List<Tuple> zrangeWithScore(String key, int start, int stop);

    List<String> zrangeByScore(String key, double min, double max);

    List<String> zrangeByScore(String key, String min, String max);

    List<String> zrevrangeByScore(String key, double max, double min);

    List<String> zrangeByScore(String key, double min, double max, int offset,
                               int count);

    List<String> zrevrangeByScore(String key, String max, String min);

    List<String> zrangeByScore(String key, String min, String max, int offset, int count);

    List<String> zrevrangeByScore(String key, double max, double min, int offset, int count);

    List<Tuple> zrangeByScoreWithScores(String key, double min, double max);

    List<Tuple> zrevrangeByScoreWithScores(String key, double max, double min);

    List<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count);

    List<String> zrevrangeByScore(String key, String max, String min, int offset, int count);

    List<Tuple> zrangeByScoreWithScores(String key, String min, String max);

    List<Tuple> zrevrangeByScoreWithScores(String key, String max, String min);

    List<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count);

    List<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count);

    List<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count);

    Long zremrangeByScore(String key, double start, double end);

    Long zremrangeByScore(String key, String start, String end);

    void zrem(String key, String member);

    void zrem(String key, String... members);

    double zincrby(String key, double score, String member);

    /**
     * 对数字类型做加1操作，实现需保证原子操作，客户端线程安全 redis实现，如果value是整形长整型以外的数据会报错
     *
     * @param key key
     * @return 返回增量操作后的数量, 如果key不存在返回null
     */
    Long increment(String key);

    /**
     * 对数字类型做增量操作，实现需保证原子操作，客户端线程安全 redis实现，如果value是整形长整型以外的数据会报错
     *
     * @param key   key
     * @param delta 增量，指定负数实现减法操作
     * @return 返回增量操作后的数量, 如果key不存在返回null
     */
    Long incrementBy(String key, long delta);

    /**
     * 对数字类型做减量操作，实现需保证原子操作，客户端线程安全 redis实现，如果value是整形长整型以外的数据会报错
     *
     * @param key
     * @return
     */
    Long decrement(String key);

    /**
     * 对数字类型做减量操作，实现需保证原子操作，客户端线程安全 redis实现，如果value是整形长整型以外的数据会报错
     *
     * @param key
     * @param decrement 减量，指定负数实际相当于加操作
     * @return
     */
    Long decrementBy(String key, long decrement);

    /**
     * 拿到key对应的数值列表,得到后数据依然存在
     *
     * @param key key
     * @return 数值列表，如果数值列表不存在，返回空表(非空指针)
     */
    List<Long> getLongList(String key);

    /**
     * 设置key对应的数值列表,默认永远不过期
     *
     * @param key  key
     * @param list 数值列表,如果原先列表存在,先删除，再重新插入
     */
    void setLongList(String key, List<Long> list);

    /**
     * 设置key对应的数值列表
     *
     * @param key     key
     * @param list    数值列表,如果原先列表存在,先删除,再重新插入
     * @param seconds 超时时间
     */
    void setLongList(String key, List<Long> list, int seconds);

    /**
     * 在列表尾追加一个数值,不改变原来的生命周期特性
     *
     * @param key    key
     * @param number 待追加数值
     */
    void addLongElement(String key, long number);

    /**
     * 在列表尾追加一个数值列表,不改变原来的生命周期特性
     *
     * @param key    key
     * @param number 带追加数值列表
     */
    void addLongList(String key, List<Long> number);

    /**
     * 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定 下标(index)参数 start 和 stop
     * 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推 你也可以使用负数下标，以 -1
     * 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推 注意LRANGE命令和编程语言区间函数的区别:
     * 假如你有一个包含一百个元素的列表，对该列表执行 LRANGE list 0 10 ，结果是一个包含11个元素的列表，这表明 stop
     * 下标也在 LRANGE 命令的取值范围之内(闭区间)，这和某些语言的区间函数可能不一致，比如Ruby的 Range.new 、
     * Array#slice 和Python的 range() 函数 超出范围的下标值不会引起错误 算法复杂度O(S+N)， S 为偏移量
     * start ， N 为指定区间内元素的数量
     *
     * @param key   key
     * @param start 开始偏移
     * @param end   结束偏移
     * @return 一个列表，包含指定区间内的元素
     */
    List<Long> lrangeLong(String key, long start, long end);

    List<String> lrange(String key, long start, long end);

    /**
     * 针对普通数值类型的操作
     */

    /**
     * 获取数值类型
     *
     * @param key key
     * @return 数值
     */
    Number getNumber(String key);

    /**
     * 获取字符串
     *
     * @param key key
     * @return 字符串
     */
    String get(String key);

    /**
     * 设置数值,默认永远不过期
     *
     * @param key   key
     * @param value 数值
     * @param <T>   泛型类型
     */
    <T extends Number> void set(String key, T value);

    /**
     * 设置数值和过期时间
     *
     * @param key     key
     * @param value   数值
     * @param seconds 过期时间
     * @param <T>     泛型信息
     */
    <T extends Number> void set(String key, T value, int seconds);

    /**
     * 得到数字类型list
     *
     * @param key key
     * @return 数值list
     */
    List<Number> getList(String key);

    /**
     * 设置数值类型list,默认永远不过期
     *
     * @param key  key
     * @param list 数值类型list
     * @param <T>  泛型信息
     */
    <T extends Number> void setList(String key, List<T> list);

    /**
     * 设置数值类型list与过期时间
     *
     * @param key     key
     * @param list    数值类型list
     * @param seconds 过期时间
     * @param <T>     泛型信息
     */
    <T extends Number> void setList(String key, List<T> list, int seconds);

    /**
     * 添加数值类型元素
     *
     * @param key     key
     * @param element 元素
     * @param <T>     泛型信息
     */
    <T extends Number> void addElement(String key, T element);

    /**
     * 添加数值类型list
     *
     * @param key  key
     * @param list 数值list
     * @param <T>  泛型信息
     */
    <T extends Number> void addList(String key, List<T> list);

    /**
     * 针对对象类型的操作
     */

    /**
     * 获取已经缓存对象
     *
     * @param key   key
     * @param clazz 对象类别信息，理由见上
     * @param <T>   对象泛型类别
     * @return 缓存的对象
     */
    <T> T get(String key, Class<T> clazz);

    <T> Map<String, T> mget(List<String> key, Class<T> clazz);

    Map<String, String> mget(List<String> key);


    <T> void mset(Map<String, T> values, Class<T> clazz);

    /**
     * @param values
     * @param clazz
     * @param expire second
     * @param <T>
     */
    <T> void mset(Map<String, T> values, Class<T> clazz, int expire);

    void mset(Map<String, String> values);

    /**
     * @param values
     * @param expire second
     */
    void mset(Map<String, String> values, int expire);

    /**
     * 缓存对象,默认永远不过期
     *
     * @param key    key
     * @param object 所要缓存对象
     * @param clazz  对象的类别信息; 木有办法,泛型类别在运行时被擦除了,还得多传一下类信息用于序列，反序列化,麻烦一下吧
     * @param <T>    对象泛型类别
     */
    <T> void set(String key, T object, Class<T> clazz);

    /**
     * 缓存字符串,默认永不过期
     *
     * @param key   key
     * @param value 索要缓存的字符串
     */
    void set(String key, String value);

    /**
     * 缓存字符串,默认永不过期
     *
     * @param key   key
     * @param value 索要缓存的字符串
     */
    String getset(String key, String value);

    /**
     * 缓存字符串,可设置过期时间
     *
     * @param key     key
     * @param value   要换成的字符串
     * @param seconds 过期时间
     */
    void set(String key, String value, int seconds);

    /**
     * 缓存对象和过期时间
     *
     * @param key     key
     * @param object  所要缓存对象
     * @param clazz   对象的类别信息; 木有办法,泛型类别在运行时被擦除了,还得多传一下类信息用于序列，反序列化,麻烦一下吧
     * @param seconds 过期时间
     * @param <T>     对象泛型类别
     */
    <T> void set(String key, T object, Class<T> clazz, int seconds);

    /**
     * 获取列表
     *
     * @param key   key
     * @param clazz 对象类型信息
     * @param <T>   泛型信息
     * @return 对象列表
     */
    <T> List<T> getList(String key, final Class<T> clazz);

    /**
     * 获取从起始点到结束点的列表信息 不包含结束点
     *
     * @param key   key
     * @param start start
     * @param end   end
     * @param clazz 对象类型信息
     * @param <T>   泛型信息
     * @return 对象列表
     */
    <T> List<T> getList(String key, long start, long end, final Class<T> clazz);

    /**
     * 列表最后插入
     *
     * @param key
     * @param value
     * @return
     */
    int rpush(String key, String value);

    /**
     * 存放一个对象
     *
     * @param key
     * @param value
     * @param <T>
     * @return
     */
    <T> int rpush(String key, T value, Class<T> clazz);

    int rpush(String key, String... value);

    /**
     * 列表最前插入
     *
     * @param key
     * @param value
     * @return
     */
    int lpush(String key, String value);

    int lpush(String key, String... value);

    /**
     * 列表最前弹出
     *
     * @param key
     * @return
     */
    String lpop(String key);

    /**
     * 返回一个对象
     *
     * @param key
     * @param clazz
     * @param <T>
     * @return
     */
    <T> T lpop(String key, Class<T> clazz);

    /**
     * 列表最后弹出
     *
     * @param key
     * @return
     */
    String rpop(String key);

    /**
     * 获取列表第一个元素
     *
     * @param key
     * @return
     */
    String lpeek(String key);

    /**
     * 获取索引位置元素
     *
     * @param key
     * @param index
     * @return
     */
    String lindex(String key, int index);


    /**
     * 设定链表中指定位置的值为新值
     *
     * @param key   key
     * @param index 索引位置
     * @param value 元素值
     * @return
     */
    String lset(String key, int index, String value);

    /**
     * 返回列表的长度
     *
     * @param key key
     * @return 列表的长度
     */
    Long getListLen(String key);

    /**
     * 截取list指定范围
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    String ltrim(String key, int start, int end);

    /**
     * 删除前count个值等于value的元素
     *
     * @param key   key
     * @param count 删除个数(count>0,从头遍历;count<0,从尾遍历;count=0,删除所有value值)
     * @param value 删除值
     * @return 删除个数(key不存在, 返回0)
     */
    Long lrem(String key, long count, String value);

    /**
     * 设置对象列表,默认永远不过期
     *
     * @param key   key
     * @param list  传入的对象列表
     * @param clazz 对象类型信息
     * @param <T>   泛型信息
     */
    <T> void setList(String key, List<T> list, final Class<T> clazz);

    /**
     * 设置对象列表与过期时间
     *
     * @param key     key
     * @param list    传入的对象列表
     * @param clazz   对象类型信息
     * @param seconds 过期时间
     * @param <T>     泛型信息
     */
    <T> void setList(String key, List<T> list, final Class<T> clazz, int seconds);

    /**
     * 对已有列表添加对象
     *
     * @param key     key
     * @param element 传入的对象
     * @param clazz   对象类型信息
     * @param <T>     泛型信息
     */
    <T> void addElement(String key, T element, final Class<T> clazz);

    /**
     * 对已有列表，一次性添加对象列表
     *
     * @param key   key
     * @param list  要添加的对象列表
     * @param clazz 对象类型信息
     * @param <T>   泛型信息
     */
    <T> void addList(String key, List<T> list, final Class<T> clazz);


    /**
     * 在集合中添加一个或多个long类型元素
     *
     * @param key    key
     * @param values 字符串数组,可变参数类型
     */
    void sadd(String key, String... values);

    /**
     * 在集合中添加一个或多个long类型元素
     *
     * @param key     key
     * @param numbers long类型数组,可变参数类型
     */
    void saddLong(String key, Long... numbers);

    /**
     * 在集合中添加一个long类型集合
     *
     * @param key       key
     * @param numberSet long类型集合
     */
    void saddLong(String key, Set<Long> numberSet);

    /**
     * 从集合中删除若干个元素,
     *
     * @param key     key
     * @param numbers 元素数组,可变参数形式
     */
    void sremLong(String key, Long... numbers);

    /**
     * 从集合中删除若干个元素
     *
     * @param key       key
     * @param numberSet 元素集合,集合形式
     */
    void sremLong(String key, Set<Long> numberSet);

    /**
     * 从集合中查找所有的元素
     *
     * @param key key
     * @return 集合中所有的元素
     */
    Set<Long> smembersLong(String key);

    /**
     * 从集合中查找所有的元素
     *
     * @param key key
     * @return 集合中所有的元素
     */
    Set<String> smembers(String key);

    /**
     * 查找某个元素是否在这个集合中
     *
     * @param key    key
     * @param number 元素
     * @return 是否在这个集合中
     */
    boolean sisMemember(String key, Long number);

    /**
     * 查找某个元素是否在这个集合中
     *
     * @param key    key
     * @param member 元素
     * @return 是否在这个集合中
     */
    boolean sisMember(String key, String member);

    /**
     * 查找某几个元素是否在这个集合中
     *
     * @param key     key
     * @param members 元素
     * @return 是否在这个集合中
     */
    Map<String, Boolean> mSisMember(String key, List<String> members);

    /**
     * 查找这个集合的基数,元素个数
     *
     * @param key key
     * @return 元素个数
     */
    Long scard(String key);


    /**
     * 返回列表 key 的长度 如果 key 不存在，则 key 被解释为一个空列表，返回 0 如果 key 不是列表类型，返回一个错误
     *
     * @param key key
     * @return 列表 key 的长度
     */
    Long llen(String key);


    /**
     * 串行访问set类型的value
     *
     * @param keyCollection key的集合
     * @return list, 元素由各个set组成
     */
    List<Set<Long>> smembersLongSerial(Collection<String> keyCollection);

    /**
     * Hash类型操作,value为对象类型
     */

    /**
     * 将哈希表 key 中的域 field 的值设为 value 如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。 如果域
     * field 已经存在于哈希表中，旧值将被覆盖 算法复杂度O(1)
     *
     * @param key   key
     * @param field hash键
     * @param value hash值
     * @return 如果 field 是哈希表中的一个新建域，并且值设置成功，返回 1 如果哈希表中域 field
     * 已经存在且旧值已被新值覆盖，返回 0
     */
    <T> Long hset(String key, String field, T value, Class<T> clazz);

    Long hset(String key, String field, String value);

    /**
     * 返回哈希表 key 中给定域 field 的值 算法复杂度O(1)
     *
     * @param key   key
     * @param field hash键
     * @return 给定域的值。 当给定域不存在或是给定 key 不存在时，返回 nil
     */
    <T> T hget(String key, String field, Class<T> clazz);

    String hget(String key, String field);

    /**
     * 将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在 若域 field 已经存在，该操作无效
     * 如果 key 不存在，一个新哈希表被创建并执行 HSETNX 命令 算法复杂度O(1)
     *
     * @param key   key
     * @param field hash键
     * @param value hash值
     * @return 设置成功，返回 1,如果给定域已经存在且没有操作被执行，返回 0
     */
    <T> Long hsetnx(String key, String field, T value, Class<T> clazz);

    Long hsetnx(String key, String field, String value);

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中 此命令会覆盖哈希表中已存在的域 如果 key
     * 不存在，一个空哈希表被创建并执行 HMSET 操作 算法复杂度O(N)， N 为 field-value 对的数量
     *
     * @param key  key
     * @param hash 整个hash结构
     * @return 如果命令执行成功，返回 OK,当 key 不是哈希表(hash)类型时，返回一个错误
     */
    <T> String hmset(String key, Map<String, T> hash, Class<T> clazz);

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中 此命令会覆盖哈希表中已存在的域 如果 key
     * 不存在，一个空哈希表被创建并执行 HMSET 操作 算法复杂度O(N)， N 为 field-value 对的数量
     * 同hmset(String, Map, Class)
     *
     * @param key
     * @param hash
     * @return 如果命令执行成功，返回 OK,当 key 不是哈希表(hash)类型时，返回一个错误
     */
    String hmset(String key, Map<String, String> hash);


    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中 此命令会先删除key， 然后再调用hmset, 最后再调用expire
     * 算法复杂度O(N)， N 为 field-value 对的数量
     * 使用pipeline
     *
     * @param key
     * @param hash
     * @param expireSeconds
     * @return 如果命令执行成功，返回 OK,当 key 不是哈希表(hash)类型时，返回一个错误
     */
    <T> String hsetAll(String key, Map<String, T> hash, Class<T> clazz, int expireSeconds) throws IllegalArgumentException;

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中 此命令会先删除key， 然后再调用hmset, 最后再调用expire
     * 算法复杂度O(N)， N 为 field-value 对的数量
     * 使用pipeline
     *
     * @param key
     * @param hash
     * @param expireSeconds
     * @return 如果命令执行成功，返回 OK,当 key 不是哈希表(hash)类型时，返回一个错误
     */
    String hsetAll(String key, Map<String, String> hash, int expireSeconds) throws IllegalArgumentException;


    /**
     * 返回哈希表 key 中，一个或多个给定域的值 如果给定的域不存在于哈希表，那么返回一个 nil 值 因为不存在的 key
     * 被当作一个空哈希表来处理，所以对一个不存在的 key 进行 HMGET 操作将返回一个只带有 nil 值的表 O(N)， N 为
     * field-value 对的数量
     *
     * @param key    key
     * @param fields 若干个hash键
     * @return 一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样
     */
    <T> List<T> hmget(String key, Class<T> clazz, String... fields);

    /**
     * 返回哈希表 key 中，一个或多个给定域的值 如果给定的域不存在于哈希表，那么返回一个 nil 值 因为不存在的 key
     * 被当作一个空哈希表来处理，所以对一个不存在的 key 进行 HMGET 操作将返回一个只带有 nil 值的表 O(N)， N 为
     * field-value 对的数量
     * 同hmget(String, Class, String...) 优化节约内存
     *
     * @param key
     * @param fields
     * @return
     */
    List<String> hmget(String key, String... fields);

    /**
     * 为哈希表 key 中的域 field 的值加上增量 increment 增量也可以为负数，相当于对给定域进行减法操作 如果 key
     * 不存在，一个新的哈希表被创建并执行 HINCRBY 命令 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0
     * 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误 本操作的值被限制在 64 位(bit)有符号数字表示之内
     * <p/>
     * 注解:修改了increment的语义,如果key不存在,那么返回null,而不是初始化为0,然后增量操作 算法复杂度O(1)
     *
     * @param key   key
     * @param field hash键
     * @param value 增量
     * @return 执行 HINCRBY 命令之后，哈希表 key 中域 field 的值
     */
    Long hincrBy(String key, String field, long value);

    /**
     * 查看哈希表 key 中，给定域 field 是否存在 算法复杂度O(1)
     *
     * @param key   key
     * @param field hash键
     * @return 如果哈希表不含有给定域，或 key 不存在，返回 0
     */
    Boolean hexists(String key, String field);

    /**
     * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略 算法复杂度O(N)， N 为要删除的域的数量
     *
     * @param key    key
     * @param fields 不定数量hash表中的键
     * @return 被成功移除的域的数量，不包括被忽略的域
     */
    Long hdel(String key, String... fields);

    /**
     * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略 算法复杂度O(N)， N 为要删除的域的数量
     *
     * @param key    key
     * @param fields hash表中的键数组
     * @return 被成功移除的域的数量，不包括被忽略的域
     */
    Long hdel(String key, Set<String> fields);

    /**
     * 返回哈希表 key 中域的数量 算法复杂度O(1)
     *
     * @param key key
     * @return 哈希表中域的数量。当 key 不存在时，返回 0
     */
    Long hlen(String key);

    /**
     * 返回哈希表 key 中的所有域 算法复杂度O(N)， N 为哈希表的大小
     *
     * @param key key
     * @return 一个包含哈希表中所有域的表, 当 key 不存在时，返回一个空表
     */
    Set<String> hkeys(String key);

    /**
     * 返回哈希表 key 中所有域的值 算法复杂度O(N)， N 为哈希表的大小
     *
     * @param key key
     * @return 一个包含哈希表中所有值的表, 当 key 不存在时，返回一个空表
     */
    <T> List<T> hvals(String key, Class<T> clazz);

    /**
     * 返回哈希表 key 中所有域的值 算法复杂度O(N)， N 为哈希表的大小
     *
     * @param key key
     * @return 一个包含哈希表中所有值的表, 当 key 不存在时，返回一个空表
     */
    List<String> hvals(String key);

    /**
     * 返回哈希表 key 中，所有的域和值 算法复杂度O(N)， N 为哈希表的大小
     *
     * @param key key
     * @return 以列表形式返回哈希表的域和域的值, 若 key 不存在，返回空列表
     */
    <T> Map<String, T> hgetAll(String key, Class<T> clazz);


    /**
     * 返回哈希表 key 中，所有的域和值 算法复杂度O(N)， N 为哈希表的大小
     *
     * @param key key
     * @return 以列表形式返回哈希表的域和域的值, 若 key 不存在，返回空列表
     */
    Map<String, String> hgetAll(String key);


    /**
     * 返回哈希表keyList中，所有的域和值 算法复杂度O(N*M)， N 为哈希表的大小, M 为keyList的大小
     * 这是借助pipeline实现的接口，对于field比较多的key慎用！！！
     *
     * @param keyList
     * @return
     */
    <T> Map<String, Map<String, T>> mhgetAll(List<String> keyList, Class<T> clazz) throws IllegalArgumentException;

    /**
     * 返回哈希表keyList中，所有的域和值 算法复杂度O(N*M)， N 为哈希表的大小, M 为keyList的大小
     * 这是借助pipeline实现的接口，对于field比较多的key慎用！！！
     *
     * @param keyList
     * @return
     */
    Map<String, Map<String, String>> mhgetAll(List<String> keyList) throws IllegalArgumentException;


    /**
     * 批量设置hash的所有域
     * 利用pipeline实现的，对于field比较多的key或者cacheMap本身比较大要慎用！！！
     *
     * @param cacheMap
     * @return
     */
    <T> Long mhsetAll(Map<String, Map<String, T>> cacheMap, Class<T> clazz, int expire) throws IllegalArgumentException;


    /**
     * 批量设置hash的所有域
     * 利用pipeline实现的，对于field比较多的key或者cacheMap本身比较大要慎用！！！
     *
     * @param cacheMap
     * @return
     */
    Long mhsetAll(Map<String, Map<String, String>> cacheMap, int expire) throws IllegalArgumentException;

}
