package com.example;

import com.example.bean.Bean1;
import com.example.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/19
 * Time: 10:28
 * Description:  intervalJoin两个流的测试代码
 * 这个里面的waterMark是按A和B的最大时间的小值来定的，例：A中最大时间为10，B中最大时间为9，则watermark为9   ，则A,B中9-5=4之前的数据全被抛弃  （这个5是lowerBound（-5，5））
 */
public class FlinkInternalJoinTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Bean2> stream1 = env.socketTextStream("tao", 8888).map(line -> {
            String[] strings = line.split(",");
            return new Bean2(strings[0], strings[1], Long.parseLong(strings[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 element, long recordTimestamp) {
                        return element.getTime() * 1000L;
                    }
                }));


        SingleOutputStreamOperator<Bean1> stream2 = env.socketTextStream("tao", 9999).map(line -> {
            String[] strings = line.split(",");
            return new Bean1(strings[0], strings[1], Long.parseLong(strings[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Bean1>) (element, recordTimestamp) -> element.getTime() * 1000L));

        SingleOutputStreamOperator<Tuple2<Bean2, Bean1>> process = stream1.keyBy(Bean2::getId)
                .intervalJoin(stream2.keyBy(Bean1::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .lowerBoundExclusive()
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Bean2, Bean1, Tuple2<Bean2, Bean1>>() {
                    @Override
                    public void processElement(Bean2 bean2, Bean1 bean1, ProcessJoinFunction<Bean2, Bean1, Tuple2<Bean2, Bean1>>.Context context, Collector<Tuple2<Bean2, Bean1>> collector) throws Exception {
                        collector.collect(new Tuple2<>(bean2, bean1));
                    }
                });

        process.print();

        env.execute();

    }
}
