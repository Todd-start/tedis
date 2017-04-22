package com.techwolf.tedis.util;

import org.apache.commons.lang3.StringUtils;
import redis.clients.util.SafeEncoder;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public final class NameSpaceUtil {

    public static final String KEY_TEMPLATE = "%s:%s";
    public static final String KEY_FOR_NS_OR_KEY_NULL_CASE = "ns_or_key_null";

    public static String combineKeyNameSpace(String ns, String key) {
        return StringUtils.isNotBlank(ns) && StringUtils.isNotBlank(key) ? String.format("%s:%s", new Object[]{ns, key}) : "ns_or_key_null";
    }

    public static byte[] combineKeyNameSpaceToByteArray(String ns, String key) {
        return SafeEncoder.encode(combineKeyNameSpace(ns, key));
    }


}
