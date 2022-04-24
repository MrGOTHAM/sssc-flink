package com.example.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/24
 * Time: 11:21
 * Description:  时间日期类，线程安全，以后可以用
 */
public class DateTimeUtil {

    private final static DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(),
                ZoneId.systemDefault());
        return formatter.format(localDateTime);
    }
    public static Long toTs(String YmDHms) {
        // 年月日+时分秒
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formatter);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
