package com.example.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/19
 * Time: 11:34
 * Description:
 */
@Data
public class OrderInfo {
    Long id;
    String order_status;
    Long user_id;
    Long province_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time; // yyyy-mm-dd HH:mm:ss
    String operate_time;
    // create_date、 create_hour可以不要
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;
}