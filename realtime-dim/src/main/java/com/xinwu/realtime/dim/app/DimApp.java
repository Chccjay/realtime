package com.xinwu.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xinwu.realtime.base.BaseApp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,4,"dim_app","topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //核心业务逻辑，对数据进行处理 实现dim层维度表的同步任务 将mysql中的数据同步到hbase中
        //对Maxwall抓取的数据进行ETL 有用的部分保留，没用的过滤
           stream.filter(new FilterFunction<String>() {
               @Override
               public boolean filter(String s) throws Exception {
                   boolean flat = false;
                   try {
                       JSONObject jsonObject = JSON.parseObject(s);
                       String database = jsonObject.getString("database");
                       String type = jsonObject.getString("type"); //update
                       JSONObject data = jsonObject.getJSONObject("data");
                       if("gmall".equals(database) && !"bootstrap-start".equals(type)
                           && !"bootstrap-complete".equals(type) && data!=null && data.size() !=0) {
                           flat = true;
                       }

                   }catch (Exception e ){
                       e.printStackTrace();
                   }
                   return flat;
               }
           });
         SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {

             @Override
             public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                 try {
                     JSONObject jsonObject = JSON.parseObject(s);
                     String database = jsonObject.getString("database");
                     String type = jsonObject.getString("type"); //update
                     JSONObject data = jsonObject.getJSONObject("data");
                     if ("gmall".equals(database) && !"bootstrap-start".equals(type)
                             && !"bootstrap-complete".equals(type) && data != null && data.size() != 0) {
                         collector.collect(jsonObject);
                     }
                 } catch (Exception e) {

                     e.printStackTrace();
                 }
             }
             });
        // kafkaSource.print();
        // 1.对读取的原始数据进行数据清洗

        // 2.使用flinkcdc读取监控配置表数据
        // 3.做成广播流
        // 4.连接主流和广播流
        // 7.筛选出要写出的字段
        // 8.写出到Hbase

    }
}
