package com.techwolf.tedis.util;

import org.apache.commons.lang3.StringUtils;
import redis.clients.util.SafeEncoder;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public final class NameSpaceUtil {

    private static final String KEY_TEMPLATE = "%s:%s";

    public static String combineKeyNameSpace(String nameSpace, String key) {
        if (StringUtils.isEmpty(nameSpace)) {
            return key;
        }

        if (StringUtils.isBlank(nameSpace) || StringUtils.isBlank(key)) {
            throw new IllegalArgumentException("StringUtils.isBlank(" + nameSpace + ") || StringUtils.isBlank(" + key + ")");
        }

        return String.format(KEY_TEMPLATE, nameSpace, key);
    }

    public static byte[] combineKeyNameSpaceToByteArray(String ns, String key) {
        return SafeEncoder.encode(combineKeyNameSpace(ns, key));
    }
}
