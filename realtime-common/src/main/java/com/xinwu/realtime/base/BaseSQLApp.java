package com.xinwu.realtime.base;

import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.util.FlinkSourceUtil;
import com.xinwu.realtime.util.SQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseSQLApp {

    public void start(int port,int parallelism,String ckAndGroupId){

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

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //4.对数据源进行处理
        handle(tableEnv,env,ckAndGroupId);//每个任务的流的处理任务不一致,需要每个任务单独处理数据

        //5.执行环境
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void createTopicDb(String ckAndGroupId, StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(SQLUtil.getKafkaTopicDb(ckAndGroupId));
    }
    //读取Hbase配置表
    public void createBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql(" CREATE TABLE hTable (\n" +
                " rowkey STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
//                " family2 ROW<q2 STRING, q3 BIGINT>,\n" +
//                " family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall:dim_base_dic',\n" +
                " 'zookeeper.quorum' = '"+ Constant.HBASE_ZOOKEEPER_QUORUM +"'\n" +
                ");\n");

    }

    public abstract void handle( StreamTableEnvironment tableEnv,StreamExecutionEnvironment env,String groupId);

}
