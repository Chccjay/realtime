package com.xinwu.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.xinwu.realtime.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {

    public static Connection getConnection() throws Exception{

        Configuration conf = new Configuration();
        conf.set("HBASE.ZOOKEEPER.QUORUM", Constant.HBASE_ZOOKEEPER_QUORUM);
        Connection connection = ConnectionFactory.createConnection(conf);

        return connection;

    }

    public static void colseConnection(Connection connection){

        if(connection !=null && !connection.isClosed()){
           try{
               connection.close();

           }catch (IOException e){
               e.printStackTrace();
           }
        }
    }

    public static void createTable(Connection connection,String nameSpace ,String table ,String... families) throws IOException {

        if (families == null || families.length==0){
            System.out.println("创建Hbase表至少一个列簇");
        }

        //1 获取Admin
          Admin admin = connection.getAdmin();
        //2 创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, table));

        for (String family :families){
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        try {
            //使用admin调用方法创建表
            admin.createTable(tableDescriptorBuilder.build());
        } catch (Exception e){
            //            e.printStackTrace();
            System.out.println("当前表格已经存在，不需要重复创建"+nameSpace+":"+table);
        }

        //关闭admin
        admin.close();

    }

    public static void dropTable(Connection connection,String namespace,String table) throws IOException {

        final Admin admin = connection.getAdmin();


        try {
            admin.disableTable(TableName.valueOf(namespace,table));
            admin.deleteTable(TableName.valueOf(namespace,table));
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    /***
     *
     * @param connection  一个同步连接
     * @param namespace   命名空间
     * @param tableName   表名
     * @param rowKey   主键
     * @param family   列族名
     * @param data    列名和列值
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {

        //1 获取Table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2 创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));

        for (String column : data.keySet()){
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(data.getString(column)));
        }
        //3 调用方法写出数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    /***
     * 删除一整行数据
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {

        //1获取Table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2 创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //3 调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();

    }


}
