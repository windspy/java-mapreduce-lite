package com.mapreducelite;

import com.mapreducelite.impl.ContextSource;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 上午10:35
 * To change this template use File | Settings | File Templates.
 */
public class Context<KEY, VALUE> {
    OutClient outClient = null;
    String jobName;
    Class keyClass;
    Class valueClass;
    public Context(String jobname){
        this.jobName = jobname;
        if (outClient==null)
            outClient = new EmRedisClient(jobname);
    }

    public Class getKeyClass() {
        return keyClass;
    }

    public void setKeyClass(Class keyClass) {
        this.keyClass = keyClass;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public void setValueClass(Class valueClass) {
        this.valueClass = valueClass;
    }

    public void write(KEY key, VALUE value, ContextSource source) {
        outClient.write(key, value, source);
    }

    public Map<KEY, List<VALUE>> loadKeys() {
        return outClient.getResults(ContextSource.MAP);
    }

    public void cleanAll(){
        outClient.clearAll();
    }

    public String getJobName() {
        return jobName;
    }
}
