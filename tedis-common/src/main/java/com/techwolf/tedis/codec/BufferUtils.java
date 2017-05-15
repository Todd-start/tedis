package com.techwolf.tedis.codec;

import com.dyuproject.protostuff.LinkedBuffer;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public class BufferUtils {
    static final int BUFFER_SIZE = 131072;
    static final ThreadLocal<LinkedBuffer> localBuffer = new ThreadLocal<LinkedBuffer>() {
        public LinkedBuffer initialValue() {
            return LinkedBuffer.allocate(BUFFER_SIZE);
        }
    };

    public BufferUtils() {
    }

    public static LinkedBuffer getApplicationBuffer() {
        return localBuffer.get();
    }
}
