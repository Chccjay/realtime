动态拆分判断维度表
（1）直接将围标做成List<String>(维表名称) 保存到代码
	 如果将代码写死后续想在修改需要重新编译上传 整个实时输出的任务也将停止
（2）将维表设计为单独的配置文件 不用保存在代码里面
	 后续想要修改 直接更改配置文件 重启任务可加载新的配置文件
（3）热修改hotfix 热加载配置文件 不需要重启 （flume 有热加载功能）
		热加载配置文件 一般以时间周期作为加载逻辑
     到此能够实现动态拆分，是时效性差一些
（4）zookeeper的watch监控节点，判断哪些维度表
	    能够存储基础的表名，但不适合存储完整的表格信息（除了要判断哪些是维度表 还要记录一些写出到habase的信息）
(5) mysql ->flinkCDC 变更数据抓取 maxwell同等功能

   分流配置表 table_process_dim 字段如下:
    1.source_table：作为数据源业务数据表名
	2.sink_table: 作为数据目的地的HBase表名
	3.sink_family:作为数据目的地的Hbase列族
	4.sink_columns:写入HBase的字段
	5.sink_row_key:写入HBase需要指定的row_key