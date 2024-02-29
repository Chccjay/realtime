package com.xinwu.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mysql.cj.jdbc.JdbcConnection;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.xinwu.realtime.base.BaseApp;
import com.xinwu.realtime.bean.TableProcessDim;
import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.dim.function.DimBroadcastFunction;
import com.xinwu.realtime.dim.function.DimHbaseSinkFunction;
import com.xinwu.realtime.util.FlinkSourceUtil;
import com.xinwu.realtime.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim_app", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //核心业务逻辑，对数据进行处理 实现dim层维度表的同步任务 将mysql中的数据同步到hbase中
        //对Maxwall抓取的数据进行ETL 有用的部分保留，没用的过滤
        // 1.对读取的原始数据进行数据清洗

        //           stream.filter(new FilterFunction<String>() {
        //               @Override
        //               public boolean filter(String s) throws Exception {
        //                   boolean flat = false;
        //                   try {
        //                       JSONObject jsonObject = JSON.parseObject(s);
        //                       String database = jsonObject.getString("database");
        //                       String type = jsonObject.getString("type"); //update
        //                       JSONObject data = jsonObject.getJSONObject("data");
        //                       if("gmall".equals(database) && !"bootstrap-start".equals(type)
        //                           && !"bootstrap-complete".equals(type) && data!=null && data.size() !=0) {
        //                           flat = true;
        //                       }
        //
        //                   }catch (Exception e ){
        //                       e.printStackTrace();
        //                   }
        //                   return flat;
        //               }
        //           }).map(JSONObject::parseObject);
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
        // 2.使用flinkcdc读取监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TBALE_NAME);
        DataStreamSource<String> mysql_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);

        //设置为1，不然变动数据抓取没有那么灵敏
        //mysql_source.print();
        // 3 在Hbase中创建维表
        SingleOutputStreamOperator<TableProcessDim> createTbaleStream = mysql_source.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {

            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取连接
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                //关闭链接
                HBaseUtil.colseConnection(connection);
            }

            @Override
            public void flatMap(String s, Collector<TableProcessDim> collector) throws Exception {
                // 使用读取配置表的数据，到Hbase中创建对应的表格
                //到Hbase中创建表格需要远程连接，在flink中远程连接需要有生命周期 所以需要用richflatmapFunction / processFunction
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String op = jsonObject.getString("op");
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        //当配置表发送一个D类型的操作 对应的Hbase需要删除一张表
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        //当配置表发送一个D类型的操作 对应的Hbase需要删除一张表
                        createTable(dim);
                    } else {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    collector.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }

            private void deleteTable(TableProcessDim dim) {

                String sinkFamily = dim.getSinkFamily();
                String[] splits = sinkFamily.split(",");

                try {
                    HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), splits);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            private void createTable(TableProcessDim dim) {

                try {
                    HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }).setParallelism(1);

        // 3.做成广播流
        // 广播状态的key用于判断是否是维度表，value用于补充信息到Hbase
        MapStateDescriptor<String, TableProcessDim> broadcast_state = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTbaleStream.broadcast(broadcast_state);

        // 4.连接主流和广播流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjStream.connect(broadcastStateStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStrem = connectStream.process(new DimBroadcastFunction(broadcast_state)).setParallelism(1);
        // 5.筛选出要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = dimStrem.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDim dim = value.f1;

                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObj.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));// 不包含的删掉

                return value;
            }
        });

        // 6.写出到Hbase
        DataStreamSink<Tuple2<JSONObject, TableProcessDim>> tuple2DataStreamSink = filterColumnStream.addSink(new DimHbaseSinkFunction());

    }
}