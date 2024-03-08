package com.xinwu.gmall.realtime.dwd.db.app;

import com.xinwu.realtime.base.BaseSQLApp;
import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {

    public static void main(String[] args) {

        new DwdTradeCartAdd().start(10013,3,"dwd_trade_card_add");
    }

    /***
     * 筛选出加购数据的明细表
     * @param tableEnv
     * @param env
     * @param groupId
     */
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        //1、获取topic数据
        createTopicDb(groupId,tableEnv);

        //2、筛选加购数据
        Table table = filterCartAdd(tableEnv);

        //3.创建kafkasink输出映射
        TableResult tableResult = getTableResult(tableEnv);
    }

    private TableResult getTableResult(StreamTableEnvironment tableEnv) {
        return tableEnv.executeSql("careate table '" + Constant.TOPIC_DWD_TRADE_CARD_ADD + "'(   " +
                "'id' String,\n" +
                "\t 'user_id' String,\n" +
                "\t 'sku_id' String,\n" +
                "\t 'cart_price' String,\n" +
                "\t sku_num String,\n" +
                "\t 'sku_name' String,\n" +
                "\t 'is_checked' String,\n" +
                "\t 'create_time' String,\n" +
                "\t 'operate_time' String,\n" +
                "\t 'is_ordered' String,\n" +
                "\t 'order_time' String,\n" +
                "\t 'source_type' String,\n" +
                "\t 'source_id' String )" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CARD_ADD));
    }

    private Table filterCartAdd(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("  SELECT \n" +
                "\t data['id'] id,\n" +
                "\t data['user_id'] user_id,\n" +
                "\t data['sku_id'] sku_id,\n" +
                "\t data['cart_price'] cart_price,\n" +
                "\t if(type='insert' ,data['sku_num'],\n" +
                "  cast(data['sku_num'] as bigint) - cast(old['sku_num'] as bigint)) sku_num,\n" +
                "\t data['sku_name'] sku_name,\n" +
                "\t data['is_checked'] is_checked,\n" +
                "\t data['create_time'] create_time,\n" +
                "\t data['operate_time'] operate_time,\n" +
                "\t data['is_ordered'] is_ordered,\n" +
                "\t data['order_time'] order_time,\n" +
                "\t data['source_type'] source_type,\n" +
                "\t data['source_id'] source_id\n" +
                "  FROM topic_db\n" +
                "  WHERE \n" +
                "  `database`='gmall'\n" +
                "  and `table` = 'cart_info'\n" +
                "  and (type ='insert' or (type='update' and old['sku_num'] is not null\n" +
                "  and cast(data['sku_num'] as bigint) > cast(old['sku_num'] as bigint)))");
    }
}
