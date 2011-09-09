package com.mapreducelite.type;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午5:44
 * To change this template use File | Settings | File Templates.
 */
public class DoubleNumber extends MRNumber {
     double value;

    public DoubleNumber(String value) {
        this.value = Double.parseDouble(value);
    }

    public double getValue() {
        return value;
    }

    public String toString(){
        return Double.toString(value);
    }
}
