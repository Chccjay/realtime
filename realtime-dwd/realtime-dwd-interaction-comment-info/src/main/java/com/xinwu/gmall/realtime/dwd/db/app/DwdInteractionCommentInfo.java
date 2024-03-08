package com.xinwu.gmall.realtime.dwd.db.app;

import com.xinwu.realtime.base.BaseSQLApp;
import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,"dwd_interaction_comment_info");

    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env ,String groupId) {

        //读取topic_db
        createTopicDb(groupId,tableEnv);

        //读取base_dic
        createBaseDic(tableEnv);

        Table commentInfo = fifterCommentInfo(tableEnv);

        tableEnv.createTemporaryView("commentInfo",commentInfo);

        //4 使用lookupjoin 维度退化
        Table joinTable = tableEnv.sqlQuery(" SELECT\n" +
                " id ,\n" +
                " user_id,\n" +
                " nick_name,\n" +
                " head_img,\n" +
                " sku_id,\n" +
                " spu_id,\n" +
                " order_id,\n" +
                " appraise appraise_code,\n" +
                " info.dic_name appraise_name,\n" +
                " comment_txt,\n" +
                " create_time,\n" +
                " operate_time\n" +
                " FROM comment_info c \n" +
                " join base_dic FOR SYSTEM_TIME OF c.proc_time as b \n" +
                " on c.appraise = b.rowKey");

        //注意，当是left join时向kafka写入数据的时候需要用到撤回流

        //5 。创建kafka sink对应的表格
        tableEnv.executeSql("create table dwd_interaction_comment_info (" +
                " id String,\n" +
                " user_id String,\n" +
                " nick_name String,\n" +
                " head_img String,\n" +
                " sku_id String,\n" +
                " spu_id String,\n" +
                " order_id String,\n" +
                " appraise_code String,\n" +
                " appraise_name String,\n" +
                " comment_txt String,\n" +
                " create_time String,\n" +
                " operate_time String" +
                ")"+ SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 6 写出到
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();

    }

    private Table fifterCommentInfo(StreamTableEnvironment tableEnv) {
        Table commentInfo = tableEnv.sqlQuery(" select\n" +
                " data['id'] id,\n" +
                " data['user_id'] user_id,\n" +
                " data['nick_name'] nick_name,\n" +
                " data['head_img'] head_img,\n" +
                " data['sku_id'] sku_id,\n" +
                " data['spu_id'] spu_id,\n" +
                " data['order_id'] order_id,\n" +
                " data['appraise'] appraise,\n" +
                " data['comment_txt'] comment_txt,\n" +
                " data['create_time'] create_time,\n" +
                " proc_time as \n" +
                " FROM topic_db\n" +
                " where `database` = 'gmall'\n" +
                " and `table` = 'comment_info'\n" +
                " and `type` = 'insert'");
        return commentInfo;
    }
}
