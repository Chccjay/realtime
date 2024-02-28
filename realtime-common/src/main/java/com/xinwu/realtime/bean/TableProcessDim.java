package com.xinwu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDim {

    //来源表
    String sourceTable;

    String sinkTable;

    String sinkColumns;

    String sinkFamily;

    //sink到Hbase 的时候主键字段
    String sinkRowKey;

    //表操作类型
    String op ;
}
