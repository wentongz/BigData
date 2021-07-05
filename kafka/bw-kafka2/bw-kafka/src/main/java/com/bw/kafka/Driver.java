package com.bw.kafka;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import java.util.Arrays;

/**
 * 作者QQ：43281991
 *
 * java -jar xxx.jar com.bw.kafka.ProducerController setting.properties users users.csv
 *
 */
public class Driver implements CommandLineRunner {

    public static void main(String[] args)  throws Exception {
        SpringApplication.run(Driver.class,args);
    }

    @Override
    public void run(String... strings) throws Exception {
        if (strings == null ||strings.length <1) {
            throw new Exception("请指定要执行的类");
        }
        //问，怎么运行ProducerController
        Object o = Class.forName(strings[0]).newInstance();
        if (o instanceof IngestionExecutor) {
            //怎么写？
            ((IngestionExecutor)o).execute(Arrays.copyOfRange(strings,1,strings.length));
        }else {
            throw new Exception("指定的程序不正确");
        }
    }
}
