package com.xinwu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.xinwu.realtime.bean.TableProcessDim;
import com.xinwu.realtime.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject,TableProcessDim,Tuple2<JSONObject,TableProcessDim>>{
    public HashMap<String, TableProcessDim> hashMap;
    public MapStateDescriptor<String,TableProcessDim> broadcast_state;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcast_state) {
        this.broadcast_state = broadcast_state;
    }

    @Override
    public void open(Configuration parameters) throws Exception {//*******************
        //预加载初始的维表信息 防止事实表数据比维表数据快的问题 从流快与主流
//                java.sql.Connection connection = DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);
        java.sql.Connection connection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(connection, "select * from gmal_cofing.table_process_dim", TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);

        }
        JdbcUtil.closeConnection(connection);


    }

    @Override
    public void processBroadcastElement(TableProcessDim value, BroadcastProcessFunction<JSONObject, TableProcessDim,
            Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcast_state);
        String op = value.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(value.getSinkTable());
            //同步删除hashMap中初始化加载的配置表信息
            hashMap.remove(value.getSourceTable());//***********
        } else {
            tableProcessState.put(value.getSourceTable(), value);
        }

    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.
            ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcast_state);
        // 查询广播状态 判断当前的数据对应的表格是否在状态里
        String tableName = value.getString("table");
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);

        //如果数据到的太早，造成的状态为空
        if(tableProcessDim == null){
            tableProcessDim = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
            //状态不为null说明当前数据是维度表数据
            collector.collect(Tuple2.of(value, tableProcessDim));
        }
    }
}
