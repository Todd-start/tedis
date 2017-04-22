package com.techwolf.tedis.codec;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by zhaoyalong on 17-3-26.
 */
public class ArrayUtils {
    /**
     * 将长整形数组转化为字符串数组
     *
     * @param numbers 长整形数组
     * @return 字符串数组
     */
    public static String[] longArrayToStringArray(Long[] numbers) {
        if (numbers != null && numbers.length > 0) {
            return Lists.transform(Arrays.asList(numbers), new Function<Long, String>() {
                public String apply(Long number) {
                    return String.valueOf(number);
                }
            }).toArray(new String[numbers.length]);
        }
        return new String[]{};
    }

    public static String[] stringCollectionToStringArray(Collection<String> collection) {
        if (collection == null) return new String[0];
        return collection.toArray(new String[collection.size()]);
    }
}
