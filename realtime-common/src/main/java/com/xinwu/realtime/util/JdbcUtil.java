package com.xinwu.realtime.util;

import com.google.common.base.CaseFormat;
import com.xinwu.realtime.constant.Constant;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        //获取jdbc连接
        // 加载驱动
        Class.forName(Constant.MYSQL_DRIVER);
        return DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);
    }

    /**
     * 执行一个查询语句，把查询结果封装T类型对象中
     */
    public static <T> List<T> queryList(Connection conn,
                                        String querySql,
                                        Class<T> tClass,
                                        boolean... isUnderlineToCamel) throws Exception {
    boolean defaultIsUToC = false; //默认不执行下划线转驼峰
    if(isUnderlineToCamel.length>0){
        defaultIsUToC = isUnderlineToCamel[0];
    }
    List<T> result = new ArrayList<>();
    //1 预编译
         PreparedStatement preparedStatement = conn.prepareStatement(querySql);
         //2 执行查询，获取结果
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        //3 解析结果 把数据封装到一个List集合中
        while (resultSet.next()){
            //变量到一行数据，把这行数据封装到一个T类型的对象中
            T t = tClass.newInstance();//使用反射创建一个T类型的对象
            //遍历这一行的没一列数值
            for (int i=1;i<=metaData.getColumnCount();i++){//从1开始数
                //获取列名
                //获取列值
                String name = metaData.getColumnLabel(i);
                Object value = resultSet.getObject(name);

                if(defaultIsUToC){//需要下划线转驼峰 a_a => aA a_aaaa_aa => aAaaaAa
                    name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,name);
                }

                //t.name = value
                BeanUtils.setProperty(t,name,value);

            }
            result.add(t);

        }

        return result;
    }

    public static void closeConnection(Connection conn){


    }
}
