package com.example.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/21
 * Time: 21:06
 * Description:
 */
public class JsonTest {

    public static void main(String[] args) {
//
//        String s = "{\"activity_reduce_amount\":0.00,\"category3_id\":86,\"category3_name\":\"平板电视\",\"coupon_reduce_amount\":0.00,\"create_date\":\"2022-04-19\",\"create_hour\":\"21\",\"create_time\":\"2022-04-19 21:03:23\",\"detail_id\":80305,\"feight_fee\":11.00,\"order_id\":26820,\"order_price\":9199.00,\"order_status\":\"1001\",\"original_total_amount\":17896.00,\"province_3166_2_code\":\"CN-NX\",\"province_area_code\":\"640000\",\"province_id\":21,\"province_iso_code\":\"CN-64\",\"province_name\":\"宁夏\",\"sku_id\":18,\"sku_name\":\"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视\",\"sku_num\":1,\"split_total_amount\":9199.00,\"spu_id\":5,\"spu_name\":\"TCL巨幕私人影院电视 4K超高清 AI智慧屏  液晶平板电视机\",\"tm_id\":4,\"tm_name\":\"TCL\",\"total_amount\":17907.00,\"user_age\":19,\"user_gender\":\"F\",\"user_id\":781}";
//        System.out.println(s);
//        OrderWide orderWide = JSON.parseObject(s, OrderWide.class);
//        System.out.println(orderWide);
//        String a = "{\"id\":\"aa\",\"name\":\"aa\",\"age\":12,\"aaa\":100}";
//        NewBean newBean = JSON.parseObject(a, NewBean.class);
//        System.out.println(newBean);

        String aa = "{\"activity_reduce_amount\":0.00,\"category3_id\":61,\"category3_name\":\"手机\",\"coupon_reduce_amount\":0.00" +
                ",\"create_date\":\"2022-04-19\",\"create_hour\":\"22\",\"create_time\":\"2022-04-19 22:30:27\",\"detail_id\":80447" +
                ",\"feight_fee\":9.00,\"order_id\":26875,\"order_price\":1299.00,\"order_status\":\"1001\",\"original_total_amount\":12123.00" +
                ",\"province_3166_2_code\":\"CN-NX\",\"province_area_code\":\"640000\",\"province_id\":21,\"province_iso_code\":\"CN-64\"" +
                ",\"province_name\":\"宁夏\",\"sku_id\":7,\"sku_name\":\"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 明月灰 游戏智能手机 小米 红米\"" +
                ",\"sku_num\":3,\"split_total_amount\":3897.00,\"spu_id\":2,\"spu_name\":\"Redmi 10X\",\"tm_id\":1,\"tm_name\":\"Redmi\",\"total_amount\":12132.00,\"user_age\":24,\"user_gender\":\"F\",\"user_id\":34}";
        JSONObject jsonObject = JSON.parseObject(aa);
        OrderWide o = JSON.parseObject(jsonObject.toJSONString(), OrderWide.class);
        System.out.println(jsonObject);
        System.out.println(o);


    }
}
