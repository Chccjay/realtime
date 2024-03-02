package com.xinwu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xinwu.realtime.base.BaseApp;
import com.xinwu.realtime.constant.Constant;
import com.xinwu.realtime.util.DateFormatUtil;
import com.xinwu.realtime.util.FlinkSinkUtil;
import org.apache.doris.shaded.org.apache.arrow.flatbuf.DateUnit;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10010, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    /***
     * log日志拆分
     * 该部分为埋点日志处理流将流拆分，使用的flinkstreamApi
     * @param env
     * @param stream
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务处理
        //1 数据清洗

        SingleOutputStreamOperator<JSONObject> jsonObStream = etl(stream);

        //2 新老访客状态标记修复 注册水位线保证数据的正常顺序

        KeyedStream<JSONObject, String> keyedStream = keyBYWithWaterMark(jsonObStream);

        SingleOutputStreamOperator<JSONObject> isNewStream = isNewFix(keyedStream);

        //3 拆分不同类型的用户日志
        // 启动日志：启动信息 报错信息
        // 页面日志：页面信息 曝光信息 动作信息 报错信息

        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errTag = new OutputTag<String>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewStream, startTag, errTag, displayTag, actionTag);

        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DSIPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));


    }

    private SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewStream, OutputTag<String> startTag, OutputTag<String> errTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewStream.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    //当前报错信息
                    context.output(errTag, err.toJSONString());
                    jsonObject.remove(err);
                }
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");

                if (start != null) {
                    //注意 输出的是
                    context.output(startTag, jsonObject.toJSONString());
                } else if (page != null) {
                    JSONArray displays = jsonObject.getJSONArray("displays");


                    if (displays != null) {

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());

                        }
                        jsonObject.remove(displays);

                    }


                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }
                        jsonObject.remove(actions);
                    }
                    //只保留page信息输出到主流
                    collector.collect(jsonObject.toJSONString());

                } else {
                    //留空

                }

            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //状态创建
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                //获取当期isnew 字段
                JSONObject common = jsonObject.getJSONObject("common");
                String is_new = common.getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String firstLoginDt = firstLoginDtState.value();
                String curDt = DateFormatUtil.tsToDate(ts);

                if ("1".equals(is_new)) {
                    //判断当前状态情况
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
                        //如果状态不为null 日期也不是今天说明数据错误，不是新访客 伪装成新访客
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        //状态为空
                        firstLoginDtState.update(curDt);
                    } else {
                        //留空
                        //当前数据是同一天新访客重复登录
                    }

                } else {
                    // is_new 为0
                    if (firstLoginDt == null) {
                        //老用户flink事实数仓里面还没有记录这个访客 需要补充访客信息
                        //把访客搜词登录日期 补充一个值，今天以前的任意一天 使用昨天的日期
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        //留空
                        //正常情况不需要修复
                    }
                }

            }
        });
    }

    private KeyedStream<JSONObject, String> keyBYWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObStream) {
        return jsonObStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");//注意有的是秒有的是毫秒 ，flink需要用毫秒
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");

                    if (page != null || start != null) {
                        if (common != null && common.getString("mid") != null && ts != null) { //当注册水位线或者keyby时当所选择的值为nullflink会报错，数据无法向下发送
                            collector.collect(jsonObject);
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();//
                    System.out.println("过滤掉脏数据" + s);
                }

            }
        });
    }
}
