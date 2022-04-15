package com.example.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.example.app.function.CustomerDeserialization;
import com.example.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/12
 * Time: 10:02
 * Description:
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


            // 生产环境需要配置这个，但是自测环境没必要
//        // 1.1 开启CK并指定状态后端为FS   memory存哪 fs存哪 rocksdb存哪
//        env.setStateBackend(new FsStateBackend("hdfs://an:8020/sssc-flink/ck"));
//        env.enableCheckpointing(5000L);             // 每五秒触发一次，生产环境一般设5分钟
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);  // 最小间隔时间


        // 2. 通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("tu")
                .port(3306)
                .username("root")
                .password("tu123456")
                .databaseList("gmall-flink-sssc")
                //.tableList("gmall-flink-sssc.base_trademark")                // 因为要监控整个库
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())                    // latest 只会打印新来的数据
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3. 打印数据 并将数据写入kafka
        streamSource.print();
        String sinkTopic ="ods_base_db";
        // 自定义kafka生产者
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        // 4. 启动任务
        env.execute("FlinkCDCWithCustomerDeserialization");
    }
}


