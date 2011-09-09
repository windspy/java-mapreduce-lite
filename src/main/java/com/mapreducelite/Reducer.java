package com.mapreducelite;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 上午10:20
 * To change this template use File | Settings | File Templates.
 */
public abstract class Reducer<KEY, VALUE> {
    protected Context context;

    public Reducer(Context context){
        this.context = context;
    }

    protected abstract void reduce(KEY key, Iterable<VALUE> values) throws IOException, InterruptedException;

    public void runReduce() throws Exception{
        Map<String, Iterable<VALUE>> keys = context.loadKeys();
        if (keys==null||keys.isEmpty()) return;
        Iterator<KEY> keyIterator = (Iterator<KEY>)keys.keySet().iterator();
        double total = keys.size();
        int i=0;
        List<Double> logged = new LinkedList<Double>();
        while (keyIterator.hasNext()){
            i++;
            double progress = i/total*100;
            KEY ke = keyIterator.next();
            Iterable<VALUE> values = keys.get(ke);
            ArrayList trans = new ArrayList();
            if (values==null) continue;
            Iterator valss = values.iterator();
            while (valss.hasNext()){
                Constructor constructor = context.getValueClass().getConstructor(String.class);
                Object res = constructor.newInstance(new Object[] { valss.next()});
                trans.add(res);
            }
            try{
                Class keclass = context.getKeyClass();
                Constructor constructor = keclass.getConstructor(String.class);
                KEY res = (KEY)constructor.newInstance(new Object[] { ke });
                reduce(res, trans);
            }catch (Exception e){
                e.printStackTrace();
            }
            if (progress%5==0&&!logged.contains(progress)){
                System.err.println("[Reduce]: complete "+progress+"%");
                logged.add(progress);
            }
        }
    }

    public void deleteAll(){
         context.cleanAll();
    }
}
