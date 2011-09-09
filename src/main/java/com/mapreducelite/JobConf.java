package com.mapreducelite;

import java.io.InputStream;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午2:52
 * To change this template use File | Settings | File Templates.
 */
public interface JobConf {
    public MRJob createJob(List<InputStream> fsinputs) throws Exception;
}
