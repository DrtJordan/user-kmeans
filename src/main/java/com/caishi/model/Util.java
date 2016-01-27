package com.caishi.model;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by root on 15-10-27.
 */
public class Util {
    public static List<Double> jsonToVector(String data){
        UserCatLike o = JSON.parseObject(data,UserCatLike.class);
        List<Double> t = new ArrayList<Double>();
        for(int i = 0; i < 104;i ++)
            t.add(0.0);
        for(CatLike cl : o.getCatLikes()){
            t.add(cl.getCatId(),cl.getWeight());
        }
       return t;
    }

    public static Map<String,List<Double>> jsonToObject(String data){
        UserCatLike o = JSON.parseObject(data,UserCatLike.class);
        List<Double> t = new ArrayList<Double>();
        for(int i = 0; i < 200;i ++)
            t.add(0.0);
        for(CatLike cl : o.getCatLikes()){
//            System.out.println(cl.getCatId()+"----------------"+cl.getWeight());
            t.set(cl.getCatId()-1,cl.getWeight());
        }
        Map<String,List<Double>> map = new HashMap<>();
        map.put(o.getId(),t);
        return map;
    }
}
