package com.mapreducelite.type;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午5:44
 * To change this template use File | Settings | File Templates.
 */
public class IntNumber extends MRNumber {
    int value;

    public IntNumber(String value) {
        this.value = Integer.parseInt(value);
    }

    public int getValue() {
        return value;
    }
    public String toString(){
        return Integer.toString(value);
    }
}
