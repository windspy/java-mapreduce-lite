package com.mapreducelite.type;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午5:44
 * To change this template use File | Settings | File Templates.
 */
public class FloatNumber extends MRNumber {
   float value;

    public FloatNumber(String value) {
        this.value = Float.parseFloat(value);
    }

    public float getValue() {
        return value;
    }
    public String toString(){
        return Float.toString(value);
    }
}
