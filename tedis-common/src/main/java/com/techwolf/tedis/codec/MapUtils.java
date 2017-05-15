package com.techwolf.tedis.codec;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by LinShunkang on 4/27/17.
 */
public final class MapUtils {

    public static <K, V> Map<K, V> createHashMap(int keyNum) {
        return new HashMap<>((int) (keyNum / 0.75) + 1);
    }

    public static <K, V> Map<K, V> createEmptyMap() {
        return new HashMap<>(0);
    }

    public static <K, V> Map<K, V> createHashMap() {
        return createHashMap(16);
    }

}
