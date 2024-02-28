package com.xinwu.realtime.constant;

public class Constant {

    //kafka
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    //msyql
    public static final String MYSQL_HOST = "";
    public static final int MYSQL_PORT = 0;
    public static final String MYSQL_USER_NAME = "";
    public static final String MYSQL_PASSWORD = "";
    public static final String MYSQL_DATABASE = "";
    public static final String MYSQL_TABLE = "";
    //分流配置表
    public static final String PROCESS_DATABASE = "";
    public static final String PROCESS_DIM_TBALE_NAME = "";

    //Hbase
    public static final String HBASE_ZOOKEEPER_QUORUM = "hadoop102,hadoop103,hadoop104";


    public static final String HBASE_NAMESPACE = "gmal";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?userSSL=false";
    public static final String TOPIC_DWD_TRAFFIC_START = "";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "";
    public static final String TOPIC_DWD_TRAFFIC_DSIPLAY = "";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "";
    public static final String TOPIC_DWD_TRADE_CARD_ADD = "";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "";




}
