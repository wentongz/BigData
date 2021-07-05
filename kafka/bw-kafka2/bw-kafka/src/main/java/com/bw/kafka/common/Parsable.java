package com.bw.kafka.common;

/**
 * 作者QQ：43281991
 * 解析数据
 */
public interface Parsable<T> {
    Boolean isHeader(String[] fields);
    Boolean isValid(String[] fields);
    T parse(String[]  fields);
}
