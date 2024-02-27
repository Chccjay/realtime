import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public class Test01 {
    public static void main(String[] args) {
        //1.构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //2.添加检查点和状态后端参数
        //1.4状态后端及检查点相关配置
        //1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        //1.4.2 开启 checkpoint
        env.enableCheckpointing(5000L);
        //1.4.3 设置checkpoint 模式：精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //1.4.4 checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/stream/" + "test01");
        //1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //1.4.6 checkpoint 之间最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //1.4.7 checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //1.4.8 取消checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        //3.读取数据

        DataStreamSource<String> kafakSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("hadoop102:9092")
                        .setTopics("topic_db")
                        .setGroupId("test01")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(), WatermarkStrategy.<String>noWatermarks(), "kafka_source");
        //4.对数据源进行处理
        kafakSource.print();

        //5.执行环境
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
