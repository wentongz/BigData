package com.bw.kafka;

import com.bw.kafka.config.LoadConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * 作者QQ：43281991
 * 发送数据到kafka
 */
public class ProducerController implements IngestionExecutor {

    @Override
    public void execute(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("参数异常");
        }
        //获取brokerURL
        //args[0]  = setting.perproties
        String brokerURL = LoadConfig.loadSetttings(args[0]).getProperty(LoadConfig.kafkaBrokerUrl);
        //指定 TopicName
        String topicName = args[1];
        //要上传的文件
        String fileName = args[2];
        Properties properties = new Properties();
        //设置服务器的url
        properties.put("bootstrap.servers",brokerURL);
        // leader ok就返回ack
        properties.put("acks","1");
        //如果失败会重新发送，
        properties.put("retries",1);
        //16k
        properties.put("batch.size",16384);
        properties.put("linger.ms",10);
        //生产者用来缓存等待发送到服务器的消息的内存总字节
        properties.put("buffer.memory",235234234);
        //key value序列化，数据走网络
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value serializer
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer(properties);
        try {
            //读取数据
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            //记录一下消息的总量
            try {
                //消息的组织 ->  key-value
                long key=0,count=0;
                String value = br.readLine();
                while (value != null) {
                    key += value.length()+1;
                    //写数据到topic
                    producer.send(new ProducerRecord<String,String>(topicName,Long.toString(key),value));
                    count++;
                    value = br.readLine();
                }
                System.out.println("消息多少条：="+count);
            }finally {
                br.close();
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
