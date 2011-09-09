package com.mapreducelite;

public class MapperRuner implements Runnable{

    Mapper mapper = null;
    String ss = null;
    public MapperRuner(Mapper mapper, String ss){
         this.mapper = mapper;
        this.ss = ss;
    }
    public void run(){
        try{
            mapper.map(ss);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
