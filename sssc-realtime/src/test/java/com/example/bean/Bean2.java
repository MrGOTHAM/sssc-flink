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
public class Bean2 {
    public Bean2(String id, String name, Long time) {
        this.id = id;
        this.name = name;
        this.time = time;
    }

    private String id;
    private String name;
    private Long time;

}
