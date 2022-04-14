package com.example.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.example.bean.TableProcess;
import com.example.common.SSSCConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/13
 * Time: 11:15
 * Description:
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private OutputTag<JSONObject> objectOutPutTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> objectOutPutTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutPutTag = objectOutPutTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化phoenix连接
        Class.forName(SSSCConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(SSSCConfig.PHOENIX_SERVER);
    }

    // value:{"db":"","tn":"","before":"{}","after":"{}","type":""}
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//        1. 获取并解析数据
        JSONObject jsonObject = JSONObject.parseObject(s);
        String after = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
//        2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable()
                    , tableProcess.getSinkColumns()
                    , tableProcess.getSinkPK()
                    , tableProcess.getSinkExtend());
        }

//        3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    // 建表语句 : create table if not exist db.tn(id varchar primary key,tm_name varchar) xxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPK, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {

            if (sinkPK == null) {
                sinkPK = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(SSSCConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];

                //判断是否为主键
                if (sinkPK.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key ");
                } else {
                    createTableSQL.append(field).append(" varchar ");
                }

                //判断是否为最后一个字段,如果不是,则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }

            createTableSQL.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());

            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            // 表如果建表失败，则没有必要往下走了
            throw new RuntimeException("Phoenix表 " + sinkTable + "建表失败");
        } finally {
        if (preparedStatement != null){
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        }
    }

    // value:{"db":"","tn":"","before":"{}","after":"{}","type":""}
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
//        1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String tableName = jsonObject.getString("tableName") + "-" + jsonObject.getString("operationType");
        TableProcess tableProcess = broadcastState.get(tableName);
        if (tableProcess != null) {
            //        2.过滤字段
            JSONObject after = jsonObject.getJSONObject("after");
            filterColumn(after, tableProcess.getSinkColumns());
            //        3.分流
            jsonObject.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
//                kafka数据写入主流
                collector.collect(jsonObject);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                // hbase数据写入测输出流
                readOnlyContext.output(objectOutPutTag, jsonObject);
            }
        } else {
            System.out.println("该组合key：" + tableName + " 不存在！");
        }
    }

    /*
       data:{"id":"11","tm_name":"asda","Logo_url":"aaa"}
       sinkColumns id, tm_name
       {"id":"11","tm_name":"asda"} 过滤后
     */
    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);
        after.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }
}
