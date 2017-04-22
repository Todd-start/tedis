package com.techwolf.tedis.codec;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhaoyalong on 17-3-26.
 */
public class SetUtils {
    /**
     * 将长整形集合转化为字符串集合
     *
     * @param numberSet 长整形集合
     * @return 字符串集合
     */
    public static Set<String> longSetToStringSet(Set<Long> numberSet) {
        if (numberSet != null && numberSet.size() > 0) {
            return Sets.newHashSet(Iterables.transform(numberSet, new Function<Long, String>() {
                public String apply(Long number) {
                    return String.valueOf(number);
                }
            }));
        }
        return new HashSet<String>();
    }

    public static String[] longSetToStringArray(Set<Long> numberSet) {
        if (numberSet != null && numberSet.size() > 0) {
            String[] array = new String[numberSet.size()];
            int idx = 0;
            for (Long item : numberSet) {
                array[idx++] = String.valueOf(item);
            }
            return array;
        }
        return new String[0];
    }

    /**
     * 将字符串集合转化为长整形集合
     *
     * @param stringSet 字符串集合
     * @return 长整形集合
     */
    public static Set<Long> stringSetToLongSet(Set<String> stringSet) {
        if (stringSet != null && stringSet.size() > 0) {
            return Sets.newHashSet(Iterables.transform(stringSet, new Function<String, Long>() {
                public Long apply(String input) {
                    if (input != null) {
                        return NumberUtils.toLong(input);
                    }
                    return null;
                }
            }));
        }
        return new HashSet<Long>();
    }
}
