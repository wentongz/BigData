package com.bw.kafka.common;

import com.bw.kafka.config.LoadConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import sun.net.www.ParseUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 作者QQ：43281991
 */
public class HBaseWriter implements Persisteable {
    //core-site.xml
    private String coreSite = null;
    //hdfs-site.xml
    private String hdfsSite = null;
    //hbase-site.xml
    private String hbaseSite = null;

    //hbase的table
    private String hbTable = null;

    //the parser
    private Parsable<Put> parser =null;

    //构造函数
    public HBaseWriter(String hbTable,Parsable<Put> parser) {
        this.hbTable = hbTable;
        this.parser =parser;
    }

    @Override
    public void initalize(Properties properties) {
        this.coreSite = properties.getProperty(LoadConfig.coreSite);
        this.hdfsSite = properties.getProperty(LoadConfig.hdfsSite);
        this.hbaseSite = properties.getProperty(LoadConfig.hbaseSite);
    }

    @Override
    public int write(ConsumerRecords<String, String> records) throws Exception {
        //写了多少条数据
        int nums =0;
        if (this.hbaseSite == null ||this.hbaseSite.isEmpty()) {
            throw new Exception("参数没有初始化");
        }
        //设置参数
        Configuration config = HBaseConfiguration.create();
        if (this.coreSite != null) {
            config.addResource(new Path(this.coreSite));
        }
        if (this.hdfsSite != null) {
            config.addResource(new Path(this.hdfsSite));
        }

        if (this.hbaseSite != null) {
            config.addResource(new Path(this.hbaseSite));
        }

        //创建hbase的连接
        Connection conn = ConnectionFactory.createConnection(config);
        try {
            Table table = conn.getTable(TableName.valueOf(this.hbTable));
            try {
                List<Put> puts = new ArrayList<>();
                //跳过第一条,表头
                long passHead = 0;
                for (ConsumerRecord<String,String> record:records) {
//                    if (passHead ==0 && this.parser.)
                    //3197468391,id_ID,1993,male,2012-10-02T06:40:55.524Z,Medan  Indonesia,480,,,,
                    String[] eles = record.value().split(",",-1);
                    if (passHead == 0 && this.parser.isHeader(eles)) {
                        passHead = 1;
                        continue;
                    }

                    if (this.parser.isValid(eles)) {
                        puts.add(this.parser.parse(eles));
                    }
                }
                if (puts.size() >0) {
                    //加入hbase
                    table.put(puts);
                }

            }finally {
                table.close();
            }

        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            conn.close();
        }
        return nums;
    }
}
