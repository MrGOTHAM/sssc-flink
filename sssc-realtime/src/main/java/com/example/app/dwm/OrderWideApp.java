package com.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.example.app.function.DimAsyncFunction;
import com.example.bean.OrderDetail;
import com.example.bean.OrderInfo;
import com.example.bean.OrderWide;
import com.example.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/19
 * Time: 11:38
 * Description:
 */
// 这个需要打开  dfs，hbase，phoenix，zk，kafka，mysql，FlinkCDC，BaseDBApp，OrderWideApp 来测试双流join，
// 主要看order_detail表中新加入的数据数量是否和orderWideApp中生成的数量一样多
// 数据流：web/app -> nginx -> springboot-> Mysql -> FlinkApp -> kafka(ods) -> FlinkApp -> Kafka/Hbase(dwd-dim) -> FlinkApp(redis) -> kafka(DWM)
// 程序           mockDB               -> mysql    ->flinkcdc ->kafka(zk) ->BaseDbApp ->kafka/phoenix(zk/hdfs/hbase) -> orderWideApp ->kafka


public class OrderWideApp {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取kafka主题的数据 并转换为JavaBean对象&提起时间戳生成WaterMark
        String oderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_new_test_group";
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(oderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dataTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dataTimeArr[0]);
                    orderInfo.setCreate_hour(dataTimeArr[1].split(":")[0]);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId)).map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        // 3. 双流Join
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))   //生产环境给的时间为最大延迟
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));


                    }
                });
        // 打印测试
        orderWideWithNoDimDS.print("orderWideWithNoDimDS>>>>>>>>>>");

        // 4. 关联维度信息 在Hbase phoenix
//        orderWideWithNoDimDS.map(orderWide->{
//            // 关联用户维度（查user_id）、地区维度(查area_id)等等
//            Long user_id = orderWide.getUser_id();
//            // 根据user_id查询Phoenix用户信息
//
//            // 将用户信息补充至orderWide
//
//            //返回结果
//            return orderWide;
//        });

//        4.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream
                .unorderedWait(orderWideWithNoDimDS
                        , new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return orderWide.getUser_id().toString();
                            }

                            @Override
                            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                                orderWide.setUser_gender(dimInfo.getString("GENDER"));
                                String birthday = dimInfo.getString("BIRTHDAY");
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                                long currentTs = System.currentTimeMillis();
                                long ts = sdf.parse(birthday).getTime();
                                long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);
                                orderWide.setUser_age((int) age);
                            }
                        }
                        , 60
                        , TimeUnit.SECONDS);

        // 4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }

                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);
        // 4.3 关联SKU维度  这个维度一定要在后面的三个维度之前关联   因为其他三个维度都在这个表中才有
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                                orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                                orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                                orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                                orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                            }

                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getSku_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        // 4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        // 4.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                                orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        // 4.6 关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS =
                AsyncDataStream.unorderedWait(
                        orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                                orderWide.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>>>");

        // 5. 将数据写入Kafka
        orderWideWithCategory3DS.map(JSONObject::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        // 6. 启动任务
        env.execute("OrderWideApp");
    }
}
