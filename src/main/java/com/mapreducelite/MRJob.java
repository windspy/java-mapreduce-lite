package com.mapreducelite;

import com.mapreducelite.type.MRType;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.List;

public class MRJob implements Runnable{
    private String name;
    private List<InputStream> fsinputs;
    private Mapper mapper;
    private Reducer reduce;
    private Context context;
    private Endler endler;

    public MRJob(String name, List<InputStream> fsinputs){
        this.name = name;
        this.fsinputs = fsinputs;
        this.context = new Context(name);
    }

    public void setMapperClass(Class<? extends Mapper> cls) throws Exception {
        Constructor constructor = cls.getConstructor(List.class, Context.class);
        mapper = (Mapper)constructor.newInstance(new Object[] { fsinputs, context});
    }

    public void setEndlerClass(Class<? extends Endler> cls) throws Exception {
        Constructor constructor = cls.getConstructor(Context.class);
        endler = (Endler)constructor.newInstance(new Object[] { context});
    }

    public void setKeyClass(Class<? extends MRType> cls) throws Exception {
        context.setKeyClass(cls);
    }

    public void setValueClass(Class<? extends MRType> cls) throws Exception {
        context.setValueClass(cls);
    }

    public void setReducerClass(Class<? extends Reducer> cls) throws Exception {
        Constructor constructor = cls.getConstructor(Context.class);
        reduce = (Reducer)constructor.newInstance(context);
    }

    public void run() {
        System.out.println("running:"+name);
        try{

            if (reduce!=null)
                reduce.deleteAll();
            if (mapper!=null)
                mapper.runMap();
            System.out.println("run maper class of:"+mapper.getClass().getName()+" completed!");
            if (reduce!=null)
                reduce.runReduce();

            if (endler!=null){
                System.out.println("run reduce class of:"+reduce.getClass().getName()+" completed!");
                 endle();
                 System.out.println("run endler class of:"+endler.getClass().getName()+" completed!");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

   public void endle(){
       try{
           InputStream is = new JobOutput().loadOutStream(name,null);
           System.err.println("start output as "+endler.getClass().getName());
           endler.endler(is);
       }catch (Exception e){
            e.printStackTrace();
       }
   }
}