package com.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.example.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/17
 * Time: 17:23
 * Description:
 */

//数据流：web/app -> nginx -> springboot -> kafka(ods) ->flinkApp -> kafka(dwd)->flinkApp->kafka(dwm)
//程序：mocklog->Nginx ->logger.sh ->kafka(zk) ->baseLogApp -> kafka->UserJumpDetailApp->kafka

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境， 与kafka分区数保持一致
        // 2. 读取kafka主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwd_user_jump_detail";


        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        // 3. 将每行数据转换为Json对象并提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));
        // 4、5、6为CEP编程 （cep是可以处理乱序数据的）
        // 4. 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        })
                // waterMark 水位线延迟两秒，即，传进来的时间是7秒，则当时水位线为5秒，此时可能会传入5-7秒内的数据（因为水位线是5秒），
                // 然后，传入第二个数据14秒，即水位线为12秒，此时还不能说明第二个数据一定是第一个数据的严格近临（因为可能出现12-14秒之间的数据）
                // next代表严格近邻，
                // 最后，在传入一个比14秒大的数据，则第一个数据将被输出，因为第一个和第二个数据一定是严格近临了
                // next就是在严格近临的情况下，才会输出

                // 超时输出要+12秒
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .within(Time.seconds(10));
                // 使用循环模式定义模式序列  和63到81行代码是一个意思
//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                return lastPageId == null || lastPageId.length() <= 0;
//            }
//        }).times(2)
//                .consecutive() // 指定严格近临，相当于next，不加这个则相当于followedBy
//                .within(Time.seconds(10));


        // 5. 将模式序列作用在流上  根据id来判断同一个人
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid")), pattern);
        // 6. 提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time_out"){};
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        // 7. UNION两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        // 8. 将数据写入kafka
        unionDS.print();
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        // 9. 启动任务
        env.execute("UserJumpDetailApp");
    }
}
