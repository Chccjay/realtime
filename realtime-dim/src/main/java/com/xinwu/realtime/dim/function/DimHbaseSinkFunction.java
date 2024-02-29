package com.xinwu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.xinwu.realtime.bean.TableProcessDim;
import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class DimHbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
    }

    @Override
    public void invoke(Tuple2<com.alibaba.fastjson.JSONObject, TableProcessDim> value, Context context) throws Exception {

        com.alibaba.fastjson.JSONObject jsonObject = value.f0;
        TableProcessDim dim = value.f1;
        //insert update delete bootstrap-insert
        String type = jsonObject.getString("type");
        com.alibaba.fastjson.JSONObject data = jsonObject.getJSONObject("data");

        if ("delete".equals(type)) {
            //删除对应的维度白哦数据
            delete(data, dim);
        } else {
            put(data, dim);
        }


    }

    @Override
    public void close() throws CloneNotSupportedException {
        HBaseUtil.colseConnection(connection);
    }

    private void put(com.alibaba.fastjson.JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        String sinkFamily = dim.getSinkFamily();
        try {
            HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue, sinkFamily, data);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void delete(com.alibaba.fastjson.JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        try {
            HBaseUtil.deleteCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
