package com.example.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/21
 * Time: 11:53
 * Description:
 */
public interface DimAsyncJoinFunction<T> {

    void join(T input, JSONObject dimInfo) throws ParseException;

    // 这个抽象方法是重点：外面参数无法传入的情况下，使用抽象方法传进来
    String getKey(T input);

}
