package com.xinwu.realtime.util;

import com.xinwu.realtime.constant.Constant;

public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return "WITH(\n" +
                "\t'connector'='kafka',\n" +
                "\t'topic'='" + topicName + "',\n" +
                "\t'properties.bootstrap.servers'='localhost:9092',\n" +
                "\t'properties.group.id'='" + groupId + "',\n" +
                "\t'scan.startup.mode'='earliest-offset',\n" +
                "\t'format'='json'\n" +
                "\t\n" +
                ")";

    }

    public static String getKafkaTopicDb(String groupId) {
        return "create table topic_db(\n" +
                "\t`database` STRING,\n" +
                "\t`table` STRING,\n" +
                "\t`data` map<STRING,STRING>,\n" +
                "\t`old` map<STRING,STRING>,\n" +
                "\t`proc_time` as PROCTIME()\n" +
                "\t`ts`\tbigint,\n" +
                "\trow_time as TO_TIMESTAMP_LTZ(ts*1000,3),\n" +
                "\t WATERMARK FOR order_time AS order_time -INTERVAL '15' SECOND\n" +
                ")\n" + getKafkaSourceSQL(Constant.TOPIC_DB, groupId);

    }

    public static String getKafkaSinkSQL(String topicName) {
        return "WITH(\n" +
                "\t'connector'='kafka',\n" +
                "\t'topic'='" + topicName + "',\n" +
                "\t'properties.bootstrap.servers'='"+Constant.KAFKA_BROKERS+"',\n" +
                "\t'format'='json'\n" +
                "\t\n" +
                ")";
    }

    /**
     * 获取upsert kafka 的链接 创建
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSQL(String topicName){
        //
        return "WITH(\n" +
                "\t'connector'='upsert-kafka',\n" +
                "\t'topic'='" + topicName + "',\n" +
                "\t'properties.bootstrap.servers'='"+Constant.KAFKA_BROKERS+"',\n" +
                "\t'key.format'='avro'\n" +
                "\t'value.format'='avro' \n" +
                ")";
    }


}
