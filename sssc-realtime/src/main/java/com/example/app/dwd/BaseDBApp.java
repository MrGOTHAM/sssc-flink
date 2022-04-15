package com.example.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.esotericsoftware.minlog.Log;
import com.example.app.function.CustomerDeserialization;
import com.example.app.function.DimSinkFunction;
import com.example.app.function.TableProcessFunction;
import com.example.bean.TableProcess;
import com.example.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;


/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/12
 * Time: 16:49
 * Description:
 */
// 数据流：web/app--->nginx----->Springboot----->FlinkAPP------>kafka(ods)----->flinkAPP----->kafka(dwd)/phoenix(dim)
// 程序：    数据流中前三个阶段使用 mockDB代替------》mysql------>flinkcdc------>kafka(zk)------>baseDBApp----->kafka/phoenix(hbase,zk,hdfs)


public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
        // 2. 消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        // 下面这行代码，搞得kafka配置相关
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        // 3. 将每行数据转换为JSON对象并过滤（去掉delete）  主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        // 取出数据的操作类型
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });
        // 4. 使用flinkCDC消费配置表并处理成      广播流
        /*
         *   table_Process配置信息表字段：                                                           后面这三个字段用于建表
         * sourceTable、 operationType（区分新增和变化的数据）、sinkType、sinkTable                    sinkColumns(列名)  pk(主键)     extend(其他)
         * base_trademark   insert                          hbase   dix_xxx(Phoenix表名) 维度表
         * order_info       insert                          kafka   dwd_xxa(主题名)       主题
         * order_info       update                          kafka   dwd_xxb
         */
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("tu")
                .port(3306)
                .username("root")
                .password("tu123456")
                .databaseList("gmall-flink-sssc-realtime")
                .tableList("gmall-flink-sssc-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableSource = env.addSource(sourceFunction);
        // 这里的String存的是tableProcess中的联合主键（source_table和operate_type）            mapStateDescriptor 主要用于状态相关的需求
        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class,TableProcess.class);

//        在日常Flink开发中，有时需要给正在运行中的Flink程序修改参数，比如过滤一些字段、字段值等，而这些值有时是配置在Mysql中的，
//        但如果进行高吞吐计算的Function中动态查询配置文件有可能使任务阻塞，甚至导致任务出现失败的情况。
//        遇到上述场景时，可以考虑通过广播流查询配置文件，广播到某个operator的所有并发实例中，然后与另一个条流数据连接进行计算。
        // mysql中的配置表一旦变更，则将立即以广播的形式发送给正在工作的flink，及时修改参数，不需要停止，且继续工作
        BroadcastStream<String> broadcastStream = tableSource.broadcast(mapStateDescriptor);
        // 5. 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);
        // 6. 分流    处理数据   广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));
        // 7. 提取kafka流数据和Hbase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        // 8. 将kafka数据写入kafka主题，将Hbase数据写入Phoenix表
        kafka.print("kafka>>>>>>>>>>>>");
        hbase.print("hbase>>>>>>>>>>>>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("after").getBytes());
            }
        }));
        // 9. 启动任务
        env.execute("BaseDBApp");
    }
}
