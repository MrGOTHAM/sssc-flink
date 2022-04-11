package com.example;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/10
 * Time: 19:44
 * Description: 使用自定义序列化类
 */
public class FlinkCDCWithCustomerDeserialization {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // 2. 通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("an")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink-sssc")
                .tableList("gmall-flink-sssc.base_trademark")                // 如果不添加该参数，则为监视这个数据库下的所有表
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.initial())                    // latest 只会打印新来的数据
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        // 3. 打印数据
        streamSource.print();

        // 4. 启动任务
        env.execute("FlinkCDCWithCustomerDeserialization");
    }
}
