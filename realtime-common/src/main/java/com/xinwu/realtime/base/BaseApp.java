package com.xinwu.realtime.base;

import com.xinwu.realtime.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    public void start(int port,int parallelism,String ckAndGroupId,String topicName){

        //1.2 获取流处理环境，并指定本地测试时启动WebUI所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);

        //1.构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        System.setProperty("HADOOP_USER_NAME","atguigu");//设置成hadoop上可以读写权限的用户，用于存储检查点

        //2.添加检查点和状态后端参数
        //1.4状态后端及检查点相关配置
        //1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        //1.4.2 开启 checkpoint
        env.enableCheckpointing(5000L);
        //1.4.3 设置checkpoint 模式：精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //1.4.4 checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/stream/" + ckAndGroupId); //当前检查点存储的位置
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
                FlinkSourceUtil.getKafkaSource(ckAndGroupId,topicName),
                WatermarkStrategy.<String>noWatermarks(),
                "kafka_source");
        //4.对数据源进行处理
        handle(env,kafakSource);//每个任务的流的处理任务不一致,需要每个任务单独处理数据

        //5.执行环境
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> kafakSource);

}
