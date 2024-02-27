package com.xinwu.realtime.util;

import com.xinwu.realtime.constant.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/***
 * 将和外部工具连接的部分都放在util里方便管理
 */
public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String groupId,String topic){
        return  KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(
                        //SimpleStringSchema 无法反序列化null值数据 会直接报错
                        //后续DWD 回向kafka发送null 不能使用 SimpleStringSchema()
                        //new SimpleStringSchema()
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if(bytes !=null && bytes.length !=0){
                                    return new String(bytes, StandardCharsets.UTF_8);
                                }
                                return "";
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return null;
                            }
                        })
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }

}
