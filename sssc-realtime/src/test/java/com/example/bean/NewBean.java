package com.example.bean;

import lombok.Data;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/21
 * Time: 22:12
 * Description:
 */
@Data
public class NewBean {

private String id;
private String name;
private int age;
private Long aaa;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Long getAaa() {
        return aaa;
    }

    public void setAaa(Long aaa) {
        this.aaa = aaa;
    }
}
