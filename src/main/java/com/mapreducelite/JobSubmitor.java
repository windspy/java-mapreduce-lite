package com.mapreducelite;

import com.mapreducelite.thread.CustomThreadPool;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午8:42
 * To change this template use File | Settings | File Templates.
 */
public class JobSubmitor {
    public static void submit(MRJob mrjob) {
        CustomThreadPool executor = new CustomThreadPool(1);
        executor.execute(mrjob);
        executor.waitFinish();
        executor.closePool();
    }
}
