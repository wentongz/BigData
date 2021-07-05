package com.bw.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 作者QQ：43281991
 * kafka -> hbase
 * 写数据的操作
 */
public interface Persisteable {
    void initalize(Properties properties);
    int  write(ConsumerRecords<String,String> records) throws Exception;
}
