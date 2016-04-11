package com.caishi.model;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by YMY on 16-3-1.
 */
public class RedisUtil {

    public static ShardedJedisPool getRedisPool(String hosts,String auth){
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(200);
        poolConfig.setMaxIdle(100);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(false);
        poolConfig.setTestOnReturn(false);
        //设置Redis信息
        String[] hs = hosts.split(",");
        List<JedisShardInfo> infoList = new ArrayList<JedisShardInfo>();
        for(String h : hs){
            String[] host_port = h.split(":");
            JedisShardInfo info = new JedisShardInfo(host_port[0], Integer.valueOf(host_port[1]), 10000);
            info.setPassword(auth);
            infoList.add(info);
        }
//        JedisShardInfo shardInfo1 = new JedisShardInfo("10.1.1.122", 6385, 10000);
//        shardInfo1.setPassword("9icaishi");
//        JedisShardInfo shardInfo2 = new JedisShardInfo("10.1.1.132", 6385, 10000);
//        shardInfo2.setPassword("9icaishi");
//        JedisShardInfo shardInfo3 = new JedisShardInfo("10.1.1.142", 6385, 10000);
//        shardInfo3.setPassword("9icaishi");


//        JedisShardInfo shardInfo1 = new JedisShardInfo("10.2.1.8", 6380, 10000);
//        shardInfo1.setPassword("bigdataservice");
//        JedisShardInfo shardInfo2 = new JedisShardInfo("10.2.1.9", 6380, 10000);
//        shardInfo2.setPassword("bigdataservice");
//        JedisShardInfo shardInfo3 = new JedisShardInfo("10.2.1.10", 6380, 10000);
//        shardInfo3.setPassword("bigdataservice");

        //初始化ShardedJedisPool
//        List<JedisShardInfo> infoList = Arrays.asList(shardInfo1, shardInfo2, shardInfo3);
//        List<JedisShardInfo> infoList = Arrays.asList(shardInfo1);
        ShardedJedisPool jedisPool = new ShardedJedisPool(poolConfig, infoList);
        return jedisPool;

    }
    public static void main(String[] args){
        String v = getRedisPool("10.2.1.8:6380,10.2.1.9:6380,10.2.1.10:6380","bigdataservice").getResource().get("UserCF_018a3c77387b462a172939d6dd25805e");
        System.out.println(v);

    }
}
