package com.huangys.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisUtil {
    private static JedisPool jedisPool = new JedisPool();

    private RedisUtil(){}

    public static Jedis getJedis(){
        return jedisPool.getResource();
    }
}

