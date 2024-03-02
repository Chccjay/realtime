package com.xinwu.realtime.util;

import com.xinwu.realtime.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class FlinkSinkUtil {
    public static KafkaSink getKafkaSink(String topicName){
     return  KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
             .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)//設置發送類型精准一次性
             .setTransactionalIdPrefix("xinwu"+topicName+System.currentTimeMillis())//设置事务id前缀
             .setProperty("transaction.time.out.ms",15*60*1000+"")//设置事务超时时间
             .build();

    }
}
