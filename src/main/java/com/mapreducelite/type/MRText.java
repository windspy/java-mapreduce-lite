package com.mapreducelite.type;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午5:43
 * To change this template use File | Settings | File Templates.
 */
public class MRText extends MRType{
    String value;

    public MRText(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public String toString(){
        return value;
    }

}
