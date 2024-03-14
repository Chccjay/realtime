package com.xinwu.gamll.realtime.dwd.db.app;

import com.xinwu.realtime.base.BaseSQLApp;
import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016,4,"dwd_trade_order_pay_suc_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        //1读取topic_db数据
        createTopicDb(groupId,tableEnv);
        //2筛选支付成功数据
        Table table = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "\t`data`['payment_type'] payment_type,\n" +
                "\t`data`['total_amount'] total_aount,\n" +
                "\t`data`['callback_time'] callback_time,\n" +
                "\t ts ,\n" +
                "row_time" +
                "\tfrom topic_db\n" +
                "\twhere `database`='gmall'\n" +
                "\tand `table`='payment_info'\n" +
                "\tand `type`='update'\n" +
                "\tand `old`['payment_status'l is not null \n" +
                "\tand `data`['payment status']='1602");

        //3读取下单详情表数据
        tableEnv.createTemporaryView("payment",table);

        tableEnv.executeSql("\tcreate table order_detail(\n" +
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
                "\t)"+ SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,groupId));
        // 4 创建base_dic 字典表
        createBaseDic(tableEnv);

        // 5 使用interval join 完成支付成功流和详情订单表的
        Table payOrderTable = tableEnv.sqlQuery("SELECT\n" +
                "\tod.id,\n" +
                "\tp.order_id,\n" +
                "\tp.user_id,\n" +
                "\tpayment_type,\n" +
                "\tcallback_time payment_time,\n" +
                "\tsku_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount.\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tp.ts\n" +
                "\tFROM payment p,order detail od\n" +
                "\tWHERE p.order id = od.order id\n" +
                "\tAND p.row_time BETWEEN od.row_time - INTERVAL '15'MINUTE AND od.row_time + INTERVAL '5'SECOND");

        // 6 使用lockup join完成维度退化
        tableEnv.createTemporaryView("payOrder",payOrderTable);

        Table resultTable = tableEnv.sqlQuery("\tSELECT\n" +
                "\tid,\n" +
                "\torder_id,\n" +
                "\tuser_id,\n" +
                "\tpayment_type payment_type_code,\n" +
                "\tinfo.dic_name payment_type_name,\n" +
                "\tpayment_time,\n" +
                "\tsku_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,sku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tts\n" +
                "\tFROM pay_order p \n" +
                "\tleft join base_dic FOR SYSTEM TIME AS OF p.proc time as b\n" +
                "\ton p.payment_type = b.rowkey");

        //7 写入到kafka
        tableEnv.executeSql("\tcreate table "+Constant.TOPIC_DWD_TRADE_ORDER_PAMENT_SUCCESS+"(\n" +
                "\tid STRING,\n" +
                "\torder_id STRIN\n" +
                "\tGuser_id STRING\n" +
                "payment_type_code STRING\n" +
                "\tpayment_type_name STRING,\n" +
                "\tpayment_time STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tprovince_id STRIN,\n" +
                "\tGactivity_id STRIN,\n" +
                "\tGactivity_rule_id STRING,\n" +
                "\tcoupon_id STRING,\n" +
                "\tsku_name STRING,\n" +
                "\torder_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "split_total_amount STRING,\n" +
                "\tsplit_activity_amount STRING,\n" +
                "\tsplit_coupon_amcunt STRING,\n" +
                "\tts bigint," +
                "PRIMARY KEY (id) NOT ENFORCED)" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAMENT_SUCCESS));//注意upsertjoin要加主键

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAMENT_SUCCESS
        ).execute();

    }
}
