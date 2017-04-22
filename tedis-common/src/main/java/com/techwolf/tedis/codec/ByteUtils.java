package com.techwolf.tedis.codec;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.util.SafeEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public class ByteUtils {

    private static final Logger logger = LoggerFactory.getLogger(ByteUtils.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
    }

    /**
     * 对象序列化中:采用ISO-8859-1编码,进行字节数组与字符串转化,统一发送字符串给redis客户端(redis客户端统一转化).
     * 为什么不用UTF-8:UTF-8自动补齐字段到8x长,从字符串转化回来,与原字节数组长度不一致,转化失败.
     * 普通字符串与字节数组转换没问题：JVM内字符串使用UTF-8编码，采用UTF-8转化没有问题
     * 为什么不直接发送字节数组给redis?redis可以接收字节数组,但要求key value都是字节数组,但是
     * 但是redis集群客户端的问题
     * ,同样的key,字符串和字节数组形式被散列到不同机器,为了避免错误,抛弃字节数组为key的接口,只采用string为key接口,所以
     * 对象存取能跟随其他接口习惯使用string形式key,需要将value(序列化得到的字节数组)从字节数组转化到string。
     */
    public static final String CHARSET_FOR_ENCODING_OBJECT = "ISO-8859-1";

    public static String byteArrayToString(byte[] bytes, String encoding) {
        if (bytes != null) {
            try {
                return new String(bytes, encoding);
            } catch (UnsupportedEncodingException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return "";
    }

    public static byte[] stringToByteArray(String text, String encoding) {
        if (StringUtils.isNotBlank(text)) {
            try {
                return text.getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return null;
    }

    @Deprecated
    public static byte[] dateToByteArray(Date date) {
        if (date != null) {
            return Longs.toByteArray(date.getTime());
        }
        return Longs.toByteArray(new Date().getTime());
    }

    @Deprecated
    public static Date byteArrayToDate(byte[] bytes) {
        if (bytes != null) {
            return new Date(Longs.fromByteArray(bytes));
        }
        return new Date(0);
    }

    /**
     * 按照jedis定义的string与byte[]之间的转换
     *
     * @param bytes 字节数组
     * @return 字符串
     */
    public static String byteArrayToString(byte[] bytes) {
        if (bytes != null) {
            return SafeEncoder.encode(bytes);
        }
        return null;
    }

    public static byte[] stringToByteArray(String text) {
        if (StringUtils.isNotBlank(text)) {
            return SafeEncoder.encode(text);
        }
        return null;
    }

    /**
     * 默认反序列化对象方法
     *
     * @param bytes 字节数组
     * @param clazz 类型信息
     * @return 对象
     */
    public static <T> T byteArrayToObject(byte[] bytes, Class<T> clazz) {
        return byteArrayToObjectBySerialStrategy(bytes, clazz, SerializableStrategy.HESSIAN);
    }

    //    public static <T> T byteArrayToObject(byte[] bytes, Class<T> clazz) {
    //        if (bytes == null) return null;
    //        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    //        HessianInput hi = new HessianInput(is);
    //        T object = null;
    //        try {
    //            object = (T) hi.readObject();
    //        } catch (IOException e) {
    //            logger.error(e.getMessage(), e);
    //        } finally {
    //            try {
    //                is.close();
    //            } catch (IOException e) {
    //                logger.error(e.getMessage(), e);
    //            }
    //        }
    //        return object;
    //    }

    /**
     * 根据序列化策略,对字节数组进行反序列化
     *
     * @param bytes    字节数组
     * @param clazz    类型信息
     * @param strategy 序列化策略
     * @param <T>      泛型信息
     * @return 反序列化得到的对象
     */
    public static <T> T byteArrayToObjectBySerialStrategy(byte[] bytes, Class<T> clazz,
                                                          SerializableStrategy strategy) {
        if (bytes == null) return null;
        switch (strategy) {
            case HESSIAN:
                return byteArrayToObjectByHessian(bytes, clazz);
            case JACKSON:
                return byteArrayToObjectByJackson(bytes, clazz);
            case PROTOBUF:
                try {
                    return byteArrayToObjectProtobuf(bytes, clazz.newInstance(), clazz);
                } catch (InstantiationException e) {
                    logger.error(e.getMessage(), e);
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage(), e);
                }
            default:
                return byteArrayToObjectByJackson(bytes, clazz);
        }
    }

    /**
     * 利用hessian来反序列化对象
     *
     * @param bytes 字节数组
     * @param clazz 类型信息
     * @return 对象
     */
    public static <T> T byteArrayToObjectByHessian(byte[] bytes, Class<T> clazz) {
        if (bytes == null) return null;
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        HessianInput hi = new HessianInput(is);
        T object = null;
        try {
            object = (T) hi.readObject();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return object;
    }

    /**
     * 用protobuf, 字节数组反序列化,通过protostuff-runtime执行对象序列化，代替java的序列化。
     *
     * @param objectBytes 字节数组
     * @param clazz       对象类型
     * @param instance    可能用来反序列化的对象实例
     * @param <T>         编译时泛型
     * @return 反序列化生成的对象
     */
    public static <T> T byteArrayToObjectProtobuf(byte[] objectBytes, T instance, Class<T> clazz) {
        Schema<T> schema = RuntimeSchema.getSchema(clazz);//会缓存本次clazz对应的schema，下次使用. schema 是线程安全对象
        if (objectBytes != null) {
            try {
                ProtostuffIOUtil.mergeFrom(objectBytes, instance, schema);
                return instance;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return null;
    }

    /**
     * 利用jackson json来反序列化对象
     *
     * @param bytes 字节数组
     * @param clazz 类型信息
     * @return 对象
     */
    public static <T> T byteArrayToObjectByJackson(byte[] bytes, Class<T> clazz) {
        if (bytes == null) return null;
        try {
            return objectMapper.readValue(bytes, clazz);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 默认序列化对象
     *
     * @param object 对象
     * @param clazz  类型信息
     * @return 字节数组
     */
    public static <T> byte[] objectToByteArray(T object, Class<T> clazz) {
        return objectToByteArrayByStrategy(object, clazz, SerializableStrategy.HESSIAN);
    }

    //    public static <T> byte[] objectToByteArray(Object object, Class<T> clazz) {
    //        if (object == null) return null;
    //        ByteArrayOutputStream os = new ByteArrayOutputStream();
    //        HessianOutput ho = new HessianOutput(os);
    //        try {
    //            ho.writeObject(object);
    //        } catch (Exception e) {
    //            logger.error(e.getMessage(), e);
    //        } finally {
    //            try {
    //                os.close();
    //            } catch (IOException e) {
    //                logger.error(e.getMessage(), e);
    //            }
    //        }
    //        return os.toByteArray();
    //    }

    /**
     * 根绝策略序列化对象
     *
     * @param object   对象
     * @param clazz    类型信息
     * @param strategy 序列化策略
     * @param <T>      泛型信息
     * @return 序列化后得到的字节数组
     */
    public static <T> byte[] objectToByteArrayByStrategy(T object, Class<T> clazz,
                                                         SerializableStrategy strategy) {
        if (object == null) return null;
        switch (strategy) {
            case HESSIAN:
                return objectToByteArrayByHessian(object, clazz);
            case PROTOBUF:
                return objectToByteArrayByProtobuf(object, clazz);
            case JACKSON:
                return objectToByteArrayByJackson(object, clazz);
            default:
                return objectToByteArrayByJackson(object, clazz);
        }
    }

    /**
     * 用protobuf, 对象序列化生成字节数组
     *
     * @param object 对象
     * @param clazz  对象类型信息
     * @param <T>    编译时泛型
     * @return 序列化得到的字节数组
     */
    public static <T> byte[] objectToByteArrayByProtobuf(T object, Class<T> clazz) {
        Schema<T> schema = RuntimeSchema.getSchema(clazz);//缓存本次clazz对应的schema，下次使用不会重复生成. schema是线程安全对象
        LinkedBuffer buffer = BufferUtils.getApplicationBuffer();
        byte[] objectBytes = null;
        try {
            objectBytes = ProtostuffIOUtil.toByteArray(object, schema, buffer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
        return objectBytes;
    }

    /**
     * 利用hessian序列化对象
     *
     * @param object 对象
     * @param clazz  类型信息
     * @param <T>    泛型信息
     * @return 序列化后的字节数组
     */
    public static <T> byte[] objectToByteArrayByHessian(T object, Class<T> clazz) {
        if (object == null) return null;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        HessianOutput ho = new HessianOutput(os);
        try {
            ho.writeObject(object);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                os.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return os.toByteArray();
    }

    /**
     * 使用jackson进行序列化
     *
     * @param object 对象
     * @param clazz  类型信息
     * @param <T>    泛型信息
     * @return 序列化得到的字节数组
     */
    public static <T> byte[] objectToByteArrayByJackson(T object, Class<T> clazz) {
        if (object == null) return null;
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 将number类型转化为字节数组
     *
     * @param number 数值类型
     * @return 字节数组
     */
    public static byte[] numberToByteArray(Number number) {
        return stringToByteArray(numberToString(number));
    }

    /**
     * 将字节数组转化number
     *
     * @param bytes 字节数组
     * @return 数值
     */
    public static Number byteArrayToNumber(byte[] bytes) {
        String text = byteArrayToString(bytes);
        return stringToNumber(text);
    }

    public static <T> T stringToObject(String text, Class<T> clazz) {
        if (StringUtils.isNotBlank(text)) {
            byte[] objectBytes = stringToByteArray(text, CHARSET_FOR_ENCODING_OBJECT);
            if (objectBytes != null) {
                return byteArrayToObject(objectBytes, clazz);
            }
        }
        return null;
    }

    public static <T> String objectToString(T object, Class<T> clazz) {
        return byteArrayToString(objectToByteArray(object, clazz), CHARSET_FOR_ENCODING_OBJECT);
    }

    public static Number stringToNumber(String text) {
        if (NumberUtils.isDigits(text)) {
            return NumberUtils.toLong(text, Long.MIN_VALUE);
        }
        return NumberUtils.toDouble(text, Double.NaN);
    }

    public static <T extends Number> String numberToString(T number) {
        return String.valueOf(number);
    }
}
