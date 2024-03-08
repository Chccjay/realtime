package com.xinwu.gmall.realtime.dwd.db.app;

import com.xinwu.realtime.base.BaseSQLApp;
import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10024,4,"dwd_trade_order_detail");
    }

    /***
     *
     * @param tableEnv
     * @param env
     * @param groupId
     */
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        //在flink中使用join，一定要添加状态存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

   //1.读取topic_db数据
        createTopicDb(groupId,tableEnv);
        //2、筛选订单详情表数据
        Table odTable = tableEnv.sqlQuery("  SELECT\n" +
                "\tdata['id'] id,\n" +
                "\tdata['order_id'] order_id,\n" +
                "\tdata['sku_id'] sku_id,\n" +
                "\tdata['sku_name'] sku_name,\n" +
                "\tdata['order_price'] order_price,\n" +
                "\tdata['sku_num'] sku_num,\n" +
                "\tdata['create_time'] create_time,\n" +
                "\tdata['split_total_amount'] split_total_amount,\n" +
                "\tdata['split_activity_amount'] split_activity_amount,\n" +
                "\tdata['split_coupon_amount'] split_coupon_amount,\n" +
                "\tts \n" +
                "\tFROM \n" +
                "\t topic_db\n" +
                "\tWHERE\n" +
                "\twhere `database` = 'gmall'\n" +
                "\t and `table` = 'order_detail'\n" +
                "\t and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail",odTable);
        //2、筛选订单信息表
        Table oiTable = tableEnv.sqlQuery(" SELECT\n" +
                "\tdata['id'] id,\n" +
                "\tdata['user_id'] user_id,\n" +
                "\tdata['provience_id'] provience_id,\n" +
                "\tts`\n" +
                "\tFROM \n" +
                "\t topic_db\n" +
                "\tWHERE\n" +
                "\twhere `database` = 'gmall'\n" +
                "\t and `table` = 'order_info'\n" +
                "\t and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info",oiTable);
        //2、筛选订单详情活动关联
        Table odaTable = tableEnv.sqlQuery("  SELECT\n" +
                "\tdata['order_detail_id'] id,\n" +
                "\tdata['activity_id'] activity_id,\n" +
                "\tdata['activity_rule_id'] activity_rule_id\n" +
                "\tFROM \n" +
                "\t topic_db\n" +
                "\tWHERE\n" +
                "\twhere `database` = 'gmall'\n" +
                "\t and `table` = 'order_detail_activity'\n" +
                "\t and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_activity",odaTable);
        //2、筛选活动优惠券详情表
        Table odcTable = tableEnv.sqlQuery("SELECT\n" +
                "\tdata['order_detail_id'] id,\n" +
                "\tdata['coupon_id'] coupon_id,\n" +
                "\tFROM \n" +
                "\t topic_db\n" +
                "\tWHERE\n" +
                "\twhere `database` = 'gmall'\n" +
                "\t and `table` = 'order_detail_coupon'\n" +
                "\t and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_coupon",odaTable);

        //将四张表join在一起
        TableResult joinTable = tableEnv.executeSql("SELECT\n" +
                "\t od.id, \n" +
                "\t order_id,\n" +
                "\t sku_id,\n" +
                "\t user_id,\n" +
                "\t provience_id,\n" +
                "\t activity_id,\n" +
                "\t activity_rule_id,\n" +
                "\t sku_name,\n" +
                "\t order_price,\n" +
                "\t sku_num,\n" +
                "\tcreate_time,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\t\n" +
                "\tFROM  order_detail od \n" +
                "\tjoin order_info oi \n" +
                "\ton od.order_id = oi.id\n" +
                "\tleft join oder_detail_activity oda \n" +
                "\ton oda.id = od.id \n" +
                "\tLeft join order_detail_coupon odc\n" +
                "\ton odc.id = od.id ");

        // 写出到kafka中 注意这里用到了left join 操作

        tableEnv.executeSql("\tcreate table '"+Constant.TOPIC_DWD_TRADE_ORDER_DETAIL+"'(\n" +
                "\t id STRING, \n" +
                "\t order_id STRING, \n" +
                "\t sku_id STRING, \n" +
                "\t user_id STRING, \n" +
                "\t provience_id STRING, \n" +
                "\t activity_id STRING, \n" +
                "\t activity_rule_id STRING, \n" +
                "\t sku_name STRING, \n" +
                "\t order_price STRING, \n" +
                "\t sku_num STRING, \n" +
                "\tcreate_time STRING, \n" +
                "\tsplit_total_amount STRING, \n" +
                "\tsplit_activity_amount STRING, \n" +
                "\tsplit_coupon_amount STRING, \n" +
                "\tts bigint,\n" +
                "\tPRIMARY KEY (id) NOT ENFORCED \n" +
                "\t)"+ SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

       //joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
