package com.xinwu.realtime.util;

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
            e.printStackTrace();
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
}
