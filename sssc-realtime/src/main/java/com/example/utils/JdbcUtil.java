package com.example.utils;

import com.alibaba.fastjson.JSONObject;
import com.example.common.SSSCConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/19
 * Time: 21:01
 * Description:
 */
public class JdbcUtil {

    // Class<T> clz 这个是传的T的类型，即Bean类    underScoreToCamel 是否下划线转驼峰
    public static <T> List<T> queryList(Connection connection, String querySql
            , Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        // 创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();

        // 预编译SQL       这个异常往外抛（抛给调用的代码），异常在这里处理不合适，因为调用的太多了
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        // 从resultSet的元信息里获取列的个数，然后for循环得到列名
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        // 解析resultSet
        while (resultSet.next()) {
            // 创建泛型对象并赋值
            T t = clz.newInstance();

            // 给泛型对象赋值  JDBC中是从1开始遍历的，不能从0
            for (int i = 1; i < columnCount + 1; i++) {
                // 获取列名
                String columnName = metaData.getColumnName(i);
                // 判断是否需要转换为驼峰命名
                if (underScoreToCamel) {
                    // 下划线转驼峰
                    columnName = CaseFormat.LOWER_UNDERSCORE
                            .to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                // 获取列值
                Object value = resultSet.getObject(i);

                // 给泛型对象赋值  (bean对象，列名，值)
                BeanUtils.setProperty(t, columnName, value);

            }
            resultList.add(t);
            //将该对象添加到集合
        }
        preparedStatement.close();
        resultSet.close();
        //返回结果集合
        return resultList;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName(SSSCConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(SSSCConfig.PHOENIX_SERVER);

        List<JSONObject> queryList = queryList(connection,
                "select * from GMALL_REALTIME.DIM_USER_INFO",JSONObject.class,false);
        for (JSONObject jsonObject:queryList){
            System.out.println(jsonObject);
        }
        connection.close();
    }
}
