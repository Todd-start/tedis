package com.techwolf.tedis.codec;

/**
 * Created by zhaoyalong on 17-3-25.
 */
public enum SerializableStrategy {
    HESSIAN,
    PROTOBUF,
    THRIFT,
    JACKSON;

    private SerializableStrategy() {
    }
}
