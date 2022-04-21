package com.example.utils;

import lombok.Synchronized;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/21
 * Time: 10:43
 * Description:
 */
// 双重校验懒汉式
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {

    }

    public static ThreadPoolExecutor getThreadPool() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(8,
                            16
                            , 1L
                            , TimeUnit.MINUTES, new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }

}
