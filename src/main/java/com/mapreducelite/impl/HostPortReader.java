package com.mapreducelite.impl;

public class HostPortReader {

    public static String[] getHostPort(String key){
        String res = "localhost:6379";
        if (!res.contains(":")) return null;
        return res.split(":");
    }
}
