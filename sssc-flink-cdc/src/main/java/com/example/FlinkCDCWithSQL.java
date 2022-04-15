package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/11
 * Time: 21:26
 * Description:
 */
public class FlinkCDCWithSQL {
    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. DDL方式建表   (只能做单表监控)
        tableEnv.executeSql("CREATE TABLE mysql_binlog (" +
                        " id STRING NOT NULL," +
                        " tm_name STRING," +
                        " logo_url STRING" +
                        ") WITH (" +
                        " 'connector' = 'mysql-cdc'," +
                        " 'hostname' = 'tu'," +
                        " 'port' = '3306'," +
                        " 'username' = 'root'," +
                        " 'password' = 'tu123456'," +
                        " 'database-name' = 'gmall-flink-sssc'," +
                        " 'table-name' = 'base_trademark'" +
                        ")");
        // 3. 查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");
        // 4.将动态表转换为流 打印  retract 为通用流   Row.class为javaBean通用类型
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();
        // 5.启动任务
        env.execute("FlinkCDCWithSQL");

    }
}
