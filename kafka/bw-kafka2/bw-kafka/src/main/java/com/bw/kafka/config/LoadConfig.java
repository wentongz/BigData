package com.bw.kafka.config;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * 作者QQ：43281991
 * 读取perperties配置文件
 */
public class LoadConfig {
    public final static String zooKeeperUrl = "zookeeperUrl";
    public final static String kafkaBrokerUrl = "kafkaBrokerUrl";
    public final static String coreSite = "coreSite";
    public final static String hdfsSite = "hdfsSite";
    public final static String hbaseSite = "hbaseSite";

    //加载文件内容到perperties
    public static Properties loadSetttings(String settingFile) throws Exception {
        Properties properties = new Properties();
        FileInputStream in = new FileInputStream(settingFile);
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
           try {
               //brokerUrl=sandbox.hortonworks.com:6667
               String line = bufferedReader.readLine();
               while (line !=null) {
                   String[] values = line.split("=");
                   if (values != null && values.length ==2) {
                       properties.put(values[0],values[1]);
                   }
                   line = bufferedReader.readLine();
               }
           }finally {
               bufferedReader.close();
           }

        }finally {
            in.close();
        }
        return properties;
    }
}
