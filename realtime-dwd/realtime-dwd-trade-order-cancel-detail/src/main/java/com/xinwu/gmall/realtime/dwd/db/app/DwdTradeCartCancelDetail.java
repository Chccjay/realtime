package com.xinwu.gmall.realtime.dwd.db.app;

import com.xinwu.realtime.base.BaseSQLApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartCancelDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartCancelDetail().start(10023, 4, "dwd_trade_cart_cancel_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        //筛选数据



    }
}
