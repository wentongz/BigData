package com.bw.kafka;

import ch.qos.logback.core.encoder.EchoEncoder;
import com.bw.kafka.common.Persisteable;
import com.bw.kafka.config.LoadConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 作者QQ：43281991
 *
 * java -jar xxxxxx.jar com.bw.userConsuer setting.proper users 'events:users'
 *
 */
public abstract class BaseConsumer implements IngestionExecutor {
    // kafka broker url
    private String kafkaBrokerUrl = null;

    // 指定消费者的 potic
    protected abstract String getKafkaTopic();

    //手动控制 commit
    protected abstract Boolean getKafkaAutoCommit();

    //每次能够从topic读取的最大记录数
    protected int getMaxPolledRecord() {
        return 5000;
    }

    //指定consumer组
    protected abstract String getKafkaConsumerGrp();

    //初始化 properties
    public void initialize(Properties properties) {
        this.kafkaBrokerUrl = properties.getProperty(LoadConfig.kafkaBrokerUrl);

        //check
        if (this.writers !=null && this.writers.length > 0) {
            for (Persisteable writer:writers) {
                writer.initalize(properties);
            }
        }
    }

    //hbase,mogodb,redis,es
    private Persisteable[] writers = null;

    //构造函数
    public BaseConsumer(Persisteable[] writes) {
        this.writers = writes;
    }

    //consumer，具体获取数据和写的操作
    protected void consume() throws Exception {
        if (this.kafkaBrokerUrl == null  || this.kafkaBrokerUrl.isEmpty()) {
            throw new Exception("kafka brokerUrl is not 初始化");
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers",this.kafkaBrokerUrl);
        properties.put("group.id",this.getKafkaConsumerGrp());
        properties.put("enable.auto.commit",this.getKafkaAutoCommit());
        //latest：从上一次结束的地方开始消费数据
        //earliest：从头开始消费数据
        properties.put("auto.offset.reset","earliest");
        //从发送请求到收到ACK确认等待的最长时间
        properties.put("request.timeout.ms","120000");
        properties.put("session.timeout.ms","180000");
        properties.put("max.poll.records",Integer.toString(this.getMaxPolledRecord()));
        //key-value 序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        //初始化topic
        List<TopicPartition> topics = Arrays.asList(new TopicPartition(getKafkaTopic(),0));

        //subscribe 订阅topic底层可以自动balance
//        consumer.subscribe();
        consumer.assign(topics);
        consumer.seek(topics.get(0),0L);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(3000);
                //记录数
                int recordsCount = (records !=null) ? records.count():0;
                if (recordsCount <= 0) {
                    Thread.sleep(3000);
                    continue;
                }
                //打印获取的记录数
                System.out.println("记录数= "+recordsCount);

                //check
                if (this.writers != null && this.writers.length > 0 && recordsCount > 0) {
                    //写数据
                    for(Persisteable write:writers) {
                        write.write(records);
                    }

                    //check
                    if (!this.getKafkaAutoCommit()) {
                        //commit，异步提交
                        consumer.commitSync();
                    }
                }
                System.out.println("   processd. 结束");
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    @Override
    public void execute(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("参数不匹配");
        }else {
            this.initialize(LoadConfig.loadSetttings(args[0]));
            //执行consume
            this.consume();
        }

    }
}
