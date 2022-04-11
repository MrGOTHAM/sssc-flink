package com.example;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/10
 * Time: 19:44
 * Description:
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 开启CK并指定状态后端为FS   memory存哪 fs存哪 rocksdb存哪
        env.setStateBackend(new FsStateBackend("hdfs://an:8020/sssc-flink/ck"));

        env.enableCheckpointing(5000L);             // 每五秒触发一次，生产环境一般设5分钟
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);  // 最小间隔时间

        // 2. 通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("an")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink-sssc")
                .tableList("gmall-flink-sssc.base_trademark")                // 如果不添加该参数，则为监视这个数据库下的所有表
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())                    // latest 只会打印新来的数据
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3. 打印数据
        streamSource.print();

        // 4. 启动任务
        env.execute("FlinkCDC");
    }
}
