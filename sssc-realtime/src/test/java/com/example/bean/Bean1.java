package com.example.bean;

import lombok.Data;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/19
 * Time: 10:28
 * Description:
 */
@Data
public class Bean1 {
    public Bean1(String id, String age, Long time) {
        this.id = id;
        this.age = age;
        this.time = time;
    }

    private String id;
    private String age;
    private Long time;
}
