package com.example;

import com.example.utils.ThreadPoolUtil;
import lombok.SneakyThrows;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/21
 * Time: 10:57
 * Description:
 */
public class ThreadPoolTest {
    public static void main(String[] args) {
        ThreadPoolExecutor threadPool = ThreadPoolUtil.getThreadPool();
        for (int i = 0; i < 10; i++) {
            threadPool.submit(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {

                    System.out.println("ancg=========="+Thread.currentThread().getName());
                    Thread.sleep(2000);
                }
            });
        }
    }
}
