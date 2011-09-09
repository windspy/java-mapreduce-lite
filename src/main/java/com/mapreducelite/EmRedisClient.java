package com.mapreducelite;

import com.mapreducelite.impl.ContextSource;
import com.mapreducelite.impl.JedisService;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 上午10:56
 * To change this template use File | Settings | File Templates.
 */
public class EmRedisClient<KEY, VALUE> extends JedisService implements OutClient<KEY, VALUE> {
    String jobname;
    public static final String F_TYPE = "MR";

    public EmRedisClient(String jobname) {
        this.jobname = jobname;
    }

    public void write(KEY key, VALUE value, ContextSource source) {
        super.save(key, value, source, jobname);
    }

    @Override
    public String getParm() {
        return "mr-redis";
    }

    public Set<KEY> keys(ContextSource source) {
        return super.abkeys(jobname, source);
    }

    public Set<VALUE> values(KEY ckey, ContextSource source) {
        return super.values(jobname, ckey, source);
    }

    @Override
    public Map<KEY, Set<VALUE>> getResults(ContextSource source) {
        Map<KEY, Set<VALUE>> maps = new HashMap<KEY, Set<VALUE>>();
        Set<KEY> keys = keys(source);
        if (keys == null || keys.isEmpty()) return null;
        for (KEY ckey : keys) {
            Set<VALUE> values = values(ckey, source);
            maps.put(ckey, values);
        }
        return maps;
    }

    public void clearAll() {
        super.del(jobname);
    }
}
