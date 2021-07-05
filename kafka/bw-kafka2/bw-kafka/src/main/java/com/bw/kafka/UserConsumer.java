package com.bw.kafka;

import com.bw.kafka.common.HBaseWriter;
import com.bw.kafka.common.Parsable;
import com.bw.kafka.common.Persisteable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 作者QQ：43281991
 */
public class UserConsumer extends BaseConsumer {

    public static class UserHBaseParser implements Parsable<Put> {
        @Override
        public Boolean isHeader(String[] fields) {
            //补充 2-6
            return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("locale"));
        }

        //校验数据
        @Override
        public Boolean isValid(String[] fields) {
            return fields.length == 7;
        }

        @Override
        public Put parse(String[] fields) {
            //rowKey -> user_id
            Put p = new Put(Bytes.toBytes(fields[0]));

            p = p .addColumn(Bytes.toBytes("profile"),Bytes.toBytes("birth_year"),Bytes.toBytes(fields[2]));
            p = p .addColumn(Bytes.toBytes("profile"),Bytes.toBytes("gender"),Bytes.toBytes(fields[3]));

            p = p .addColumn(Bytes.toBytes("region"),Bytes.toBytes("locale"),Bytes.toBytes(fields[1]));
            p = p .addColumn(Bytes.toBytes("region"),Bytes.toBytes("location"),Bytes.toBytes(fields[5]));
            p = p .addColumn(Bytes.toBytes("region"),Bytes.toBytes("time_zone"),Bytes.toBytes(fields[6]));

            p = p .addColumn(Bytes.toBytes("registration"),Bytes.toBytes("joinedAt"),Bytes.toBytes(fields[4]));

            return p;
        }
    }

    public UserConsumer() {
//        super(new Persisteable[]{new HBaseWriter("evnets:users",new UserHBaseParser()),new RedisWri....});
        super(new Persisteable[]{new HBaseWriter("events_db:users",new UserHBaseParser())});
    }

    @Override
    protected String getKafkaTopic() {
        return "users4";
    }

    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }

    @Override
    protected String getKafkaConsumerGrp() {
        return "userConsumerGrp";
    }
}
