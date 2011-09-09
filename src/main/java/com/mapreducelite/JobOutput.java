package com.mapreducelite;

import com.mapreducelite.impl.ContextSource;
import java.io.*;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午4:37
 * To change this template use File | Settings | File Templates.
 */
public class JobOutput<KEY, VALUE> {
    public InputStream loadOutStream(String jobName, String product) throws Exception {
        Map<KEY, List<VALUE>> keys = new EmRedisClient(jobName).getResults(ContextSource.REDUCE);
        if (keys == null || keys.isEmpty()) return null;
        StringBuffer sb = new StringBuffer();
        Iterator<KEY> keyIterator = keys.keySet().iterator();
        while (keyIterator.hasNext()) {
            KEY ke = keyIterator.next();
            String keystr = ke.toString();
            if (product != null && !keystr.endsWith("-" + product)) continue;
            if (product != null)
                keystr = keystr.replaceAll("-" + product, "");
            Iterable<VALUE> values = keys.get(ke);
            if (values == null) continue;
            sb.append(keystr + "\t");
            for (VALUE value : values)
                sb.append(value.toString());
            sb.append("\n");
        }
        System.out.println("loading jobdata of:" + jobName + " completed.");
        byte[] bytes = sb.toString().getBytes("utf-8");
        InputStream is = new ByteArrayInputStream(bytes);
        return is;
    }
}
