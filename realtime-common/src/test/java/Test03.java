import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test03 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("create table topic_db(\n" +
                "\t`database` STRING,\n" +
                "\t`table` STRING,\n" +
                "\t`ts`\tbigint,\n" +
                "\t`data` map<STRING,STRING>,\n" +
                "\t`old` map<STRING,STRING>\n" +
                ")WITH(\n" +
                "\t'connector'='kafka',\n" +
                "\t'topic'='topic_db',\n" +
                "\t'properties.bootstrap.servers'='localhost:9092',\n" +
                "\t'properties.group.id'='test03',\n" +
                "\t'scan.startup.mode'='earliest-offset',\n" +
                "\t'format'='json'\n" +
                "\t\n" +
                ")");



      tableEnv.executeSql("create table base_dic(\n" +
              "\t`dic_code` BIGINT,\n" +
              "\t`dic_name` STRING,\n" +
              "\t`parent_code`\tSTRING,\n" +
              "\t`create_time` STRING,\n" +
              "\t`operate_time` STRING\n" +
              ")WITH(\n" +
              "\t'connector'='jdbc',\n" +
              "\t'topic'='topic_db',\n" +
              "\t'url'='jdbc:mysql://hadoop102:3306/gmall',\n" +
              "\t'table-name'='base_dic'\n" +
              "\t'driver'='com.mysql.cj.jdbc.Driver'\n" +
              "\t'username'='root',\n" +
              "\t'password'='000000'\n" +
              ")\n");
      //过滤出comment_info的对应信息

        Table commentInfo = tableEnv.sqlQuery("select \n" +
                " id ,\n" +
                " user_id,\n" +
                " nick_name,\n" +
                " head_img,\n" +
                " sku_id,\n" +
                " spu_id,\n" +
                " order_id,\n" +
                " appraise,\n" +
                " comment_txt,\n" +
                " create_time,\n" +
                " operate_time\n" +
                " FROM topic_db\n" +
                " where `databse` = 'gmall'\n" +
                " and `table` = 'comment_info'\n" +
                " and `type` = 'insert'");
        tableEnv.createTemporaryView("comment_info",commentInfo);//创建视图


        tableEnv.executeSql(" SELECT\n" +
                " id ,\n" +
                " user_id,\n" +
                " nick_name,\n" +
                " head_img,\n" +
                " sku_id,\n" +
                " spu_id,\n" +
                " order_id,\n" +
                " appraise appraise_code,\n" +
                " b.dic_name appraise_name,\n" +
                " comment_txt,\n" +
                " create_time,\n" +
                " operate_time\n" +
                " FROM comment_info c \n" +
                " join base_dic b \n" +
                " on c.appraise = b.dic_code").print();
        }

}
