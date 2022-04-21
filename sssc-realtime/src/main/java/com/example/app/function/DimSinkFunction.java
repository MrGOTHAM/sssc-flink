package com.example.app.function;

import com.alibaba.fastjson.JSONObject;
import com.example.common.SSSCConfig;
import com.example.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/14
 * Time: 20:22
 * Description:维度sinkFunction, 富函数有生命周期，可以开关jdbc连接
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(SSSCConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(SSSCConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    // value:{"sinkTable":"dim_base_trademark","database":"sssc-flink-realtime",
    //"before":{"tm_name":"aaa","id":12},"after":{"tm_name":"aaa","id":12},"type":"update",
    // "tableName":"base_trademark"}
    // phoenix SQL: upsert into db.tn(id,tm_name) values('...','...')
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            // 获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(value.getString("sinkTable"),
                    after);
            System.out.println(upsertSql);
            // 预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 判断如果当前数据为更新操作，则先删除Redis中的数据
//            if ("update".equals(value.getString("type"))) {
//                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
//            }
            //执行写入操作
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    //      {"tm_name":"aaa","id":12}
//    phoenix SQL: upsert into db.tn(id,tm_name) values('...','...')
    private String genUpsertSql(String sinkTable, JSONObject data) {
        Set<String> strings = data.keySet();
        Collection<Object> values = data.values();

        // strings.mkString(",")  --->"id,tm_name"
        // StringUtils.join(strings, ",")       这两个是一个意思，上面那个是scala


        return "upsert into " + SSSCConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(strings, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";

    }
}
