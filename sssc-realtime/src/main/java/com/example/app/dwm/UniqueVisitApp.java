package com.example.app.dwm;
/*
    做用户单日访问状态存储的类
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.example.utils.MyKafkaUtil;
import lombok.val;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取kafka  dwd_page_log 主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3. 将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        // 4.　过滤数据　　　状态编程　　　只保留每个ｍｉｄ每天一次登陆的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jso -> jso.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            // 在这里面状态编程
            private ValueState<String> dateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                dateState = getRuntimeContext().getState(valueStateDescriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override   // 返回false是去除数据
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 取出上一跳 信息
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length()<=0){
                    // 取出状态数据
                    String lastDate = dateState.value();
                    // 取出今天的日期
                    String curDate = sdf.format(jsonObject.getLong("ts"));
                    // 判断两个日期是否相同
                    if (!curDate.equals(lastDate)){
                        // 不相等： 更新日期，返回true
                        dateState.update(curDate);
                        return true;
                    }
                }
                return false;
            }
        });
        // 5. 将数据写入kafka
        uvDS.print();
        // 先转为String类型，再addSink
        uvDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        // 6. 启动任务
        env.execute();

    }


}
