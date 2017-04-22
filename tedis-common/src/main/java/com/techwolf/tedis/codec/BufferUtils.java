package com.techwolf.tedis.codec;

import com.dyuproject.protostuff.LinkedBuffer;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public class BufferUtils {
    static final int bufferSize = 131072;
    static final ThreadLocal<LinkedBuffer> localBuffer = new ThreadLocal() {
        public LinkedBuffer initialValue() {
            return LinkedBuffer.allocate(131072);
        }
    };

    public BufferUtils() {
    }

    public static LinkedBuffer getApplicationBuffer() {
        return (LinkedBuffer)localBuffer.get();
    }
}
