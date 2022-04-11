package com.example;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Locale;


/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/11
 * Time: 22:06
 * Description: 自定义反序列化类
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    // 封装的数据格式
    /*
     *  Json格式更好处理
     * {
     * "database":"",
     * "tableName":"",
     * "operationType":"c u d",
     * "before":"{json}"
     * "after":"{json}"
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 1. 创建json对象用于存储最终数据
        JSONObject result = new JSONObject();
        /*
        SourceRecord{sourcePartition={server=mysql_binlog_source},
        sourceOffset={file=mysql-bin.000003, pos=1065, row=1, snapshot=true}}
         ConnectRecord{topic='mysql_binlog_source.gmall-flink-sssc.base_trademark',
         kafkaPartition=null, key=Struct{id=10}, keySchema=Schema{mysql_binlog_source.gmall_flink_sssc.base_trademark.Key:STRUCT},
         value=Struct{after=Struct{id=10,tm_name=欧莱雅,logo_url=/static/default.jpg},
         source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=gmall-flink-sssc,
         table=base_trademark,server_id=0,file=mysql-bin.000003,pos=1065,row=0},
         op=c,ts_ms=1649668457436}, valueSchema=Schema{mysql_binlog_source.gmall_flink_sssc.base_trademark.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
         */
        // 2. 获取库名表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];
        // 3. 获取before数据   struct为kafka下的
        Struct value = (Struct)sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null){
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (int i = 0; i < beforeFields.size(); i++) {
                Field field = beforeFields.get(i);
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }
        }

        // 4. 获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null){
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (int i = 0; i < afterFields.size(); i++) {
                Field field = afterFields.get(i);
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }
        // 5. 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String op = operation.toString().toLowerCase();
        if (op.equals("create")){
            op = "insert";
        }
        // 6. 将字段写入Json对象
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("operationType",op);

        // 7. 输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
