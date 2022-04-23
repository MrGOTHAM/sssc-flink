package com.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.bean.OrderDetail;
import com.example.bean.OrderWide;
import com.example.bean.PaymentInfo;
import com.example.bean.PaymentWide;
import com.example.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/21
 * Time: 16:35
 * Description:
 */

// 一个傻逼问题    如果javaBean带构造器，而且没有 ClassName(){} 这个构造器，则fastJson无法通过 JSON.parseObject(string, ClassName.class)解析出来 ，会全部置为null
// 数据流：web/app -> nginx -> springboot-> Mysql -> FlinkApp -> kafka(ods) -> FlinkApp
// -> Kafka/Hbase(dwd-dim) -> FlinkApp(redis) -> kafka(DWM)->FlinkApp->kafka(dwm)
// 程序           mockDB               -> mysql    ->flinkcdc ->kafka(zk) ->BaseDbApp ->kafka/phoenix(zk/hdfs/hbase) -> orderWideApp ->kafka->paymentWideApp ->Kafka

    // 测数据有没有丢，需要在mysql数据库中查 select * from payment_info p join order_detail o on p.order_id = o.order_id;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 读取kafka主题的数据创建流 并转换为JavaBean对象 提取时间戳生成waterMark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new
                                        SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    String time1 = element.getCreate_time();
                                    if (time1 == null){
                                        return recordTimestamp;
                                    }

                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    // 时间获取异常，则返回这个时间 recordTimestamp是进入系统的时间
                                    return recordTimestamp;
                                }
                            }
                        }));


        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId)).map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    // 时间获取异常，则返回这个时间 recordTimestamp是进入系统的时间
                                    return recordTimestamp;
                                }
                            }
                        }));
        // 3. 双流Join     先有订单 后有支付，所以窗口-15ms  如果用订单表关联支付表，则窗口+15ms
        SingleOutputStreamOperator<PaymentWide> paymentWideDs = paymentInfoDS
                .keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context context, Collector<PaymentWide> collector) {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
        // 4. 将数据写入Kafka
        paymentWideDs.print(">>>>>>>>>>>>>");
        paymentWideDs.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));


        // 5. 启动任务
        env.execute("PaymentWideApp");
    }
}
