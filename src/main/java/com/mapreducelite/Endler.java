package com.mapreducelite;

import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-25
 * Time: 下午3:59
 * To change this template use File | Settings | File Templates.
 */
public abstract class Endler {
    protected Context context;
    public Endler(Context context){
        this.context = context;
    }
    protected abstract void endler(InputStream inputStream) throws Exception;
}
