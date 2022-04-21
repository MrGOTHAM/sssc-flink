package com.example.utils;

import com.alibaba.fastjson.JSONObject;
import com.example.common.SSSCConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/20
 * Time: 9:35
 * Description:
 */
/*
p100
DimUtil和DimSinkFunction 注释了Redis，因为服务器没配，就把redis注释掉了
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 查询phoenix之前先查询Redis
        /*
        redis：
            1.存什么数据     jsonStr
            2.使用什么类型    String or Hash
            3.RedisKey      String：tableName+id     hash：外面用tableName 里面用id
            4.不选Hash的原因：    1.用户维度数据量大      2.redis需要设置过期时间

         */

//        Jedis jedis = RedisUtil.getJedis();
        // DIM_USER_INFO
//        String redisKey = "DIM:" + tableName + ":" + id;
//        String dimInfoJsonStr = jedis.get(redisKey);
//        if (dimInfoJsonStr != null) {
//
//            //重置过期时间
//            jedis.expire(redisKey, 24 * 60 * 60);
              // 归还连接
//            jedis.close();
//            //返回结果
//            return JSONObject.parseObject(dimInfoJsonStr);
//        }

        // 拼接查询语句
        String sql = "select * from " + SSSCConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";

        // 查询phoenix
        List<JSONObject> resultLists = JdbcUtil.queryList(connection, sql, JSONObject.class, false);

        // 返回结果
        JSONObject dimInfoJson = resultLists.get(0);

        // 在返回结果之前，将数据写入redis
//        jedis.set(redisKey, dimInfoJson.toJSONString());
//        jedis.expire(redisKey, 24 * 60 * 60);
//        jedis.close();
        // 返回结果
        return dimInfoJson;

    }

    // 删除redis数据
    public static void delRedisDimInfo(String tableName, String id) {

        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();

    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 初始化phoenix连接
        Class.forName(SSSCConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(SSSCConfig.PHOENIX_SERVER);

        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "4001"));

        connection.close();
    }

}
