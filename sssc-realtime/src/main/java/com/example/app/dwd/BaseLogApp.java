package com.example.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/12
 * Time: 11:10
 * Description:
 */
    //数据流：web/app -> nginx -> springboot -> kafka(ods) ->flinkApp -> kafka(dwd)
    //程序：mocklog->Nginx ->logger.sh ->kafka(zk) ->baseLogApp -> kafka


public class BaseLogApp {


    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.消费ods_base_log 主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3.将每行数据转换为JSON对象   防止脏数据，所以用process
        OutputTag<String> outputTag = new OutputTag<String>("Dirty"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                try {
                    JSONObject js = JSON.parseObject(s);
                    collector.collect(js);
                } catch (Exception e) {       // 如果数据无法解析成JSON，则将其视为脏数据并存入outputTag
                    // 发生异常，将数据写入侧输出流
                    context.output(outputTag, s);
                }
            }
        });
        // 打印脏数据
        jsonObjDS.getSideOutput(outputTag).print("dirty>>>>>>>>>");


        // 4.新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        // 获取数据中的is_new标记
                        String isNew = value.getJSONObject("common").getString("is_new");
                        // 判断isNew是否为1，不是1则不是新用户，可以直接返回，是1则不一定是新用户（可能是卸载重装了）
                        if (isNew.equals("1")) {
                            String state = valueState.value();
                            if (state != null) {
                                value.getJSONObject("common").put("is_new", "0");
                                return value;
                            } else {
                                valueState.update("1");
                            }
                        }
                        return value;
                    }
                });


        // 5.分流 侧输出流 页面：主流  启动：侧输出流   曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                // 启动日志字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    // 将数据写入页面日志主流
                    collector.collect(jsonObject.toJSONString());

                    // 提出数据中的曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {

                        String pageId = jsonObject.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            // 添加页面id
                            display.put("page_id", pageId);

                            // 将数据写出到曝光侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        // 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        // 7.将三个流进行打印并输出到对应的kafka主题中
        startDS.print("start>>>>>>>>>");
        pageDS.print("page>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        // 8.启动任务
        env.execute("BaseLogApp");

    }
}
