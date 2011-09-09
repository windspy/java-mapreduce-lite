package com.mapreducelite;

import com.mapreducelite.thread.CustomThreadPool;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 上午10:20
 * To change this template use File | Settings | File Templates.
 */
public abstract class Mapper<KEY, VALUE> {
    private List<InputStream> fsinputs;
    protected Context context;

    public Mapper(List<InputStream> fsinputs, Context context) {
        this.fsinputs = fsinputs;
        this.context = context;
    }

    public void runMap() throws Exception {

        int i = 0;
        double sum = fsinputs.size();

        for (InputStream curfs : fsinputs) {
            CustomThreadPool threadPool = new CustomThreadPool(15);
            if (curfs == null) continue;
            BufferedReader bf = new BufferedReader(new InputStreamReader(curfs));
            String ss = null;
            try {
                while ((ss = bf.readLine()) != null) {
                    MapperRuner thisRunner = new MapperRuner(this, ss);
                    threadPool.execute(thisRunner);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            curfs.close();
            bf.close();
            i++;
            threadPool.waitFinish(); //等待所有任务执行完毕
            threadPool.closePool(); //关闭线程池
            double progress = i / sum * 100;
            double d = new BigDecimal(progress).setScale(1, BigDecimal.ROUND_HALF_UP).doubleValue();
            System.err.println("[Mapper]: complete " + d + "%");
        }


    }

    protected abstract void map(String line) throws IOException, InterruptedException;

}
