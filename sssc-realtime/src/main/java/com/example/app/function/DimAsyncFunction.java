package com.example.app.function;

import com.alibaba.fastjson.JSONObject;
import com.example.common.SSSCConfig;
import com.example.utils.DimUtil;
import com.example.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/21
 * Time: 10:38
 * Description:
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(SSSCConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(SSSCConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // 获取查询的主键
                    String id = getKey(input);
                    // 查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    // 补充维度信息
                    if (dimInfo != null){
                        join(input,dimInfo);
                    }
                    // 将数据输出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }



    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}