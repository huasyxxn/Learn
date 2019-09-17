package com.huangys.flink.content.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Account2PutMapFunction extends RichMapFunction<String,Put> {
    private JedisPool jedisPool;

    @Override
    public void open(Configuration config){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(jedisPoolConfig,"10.80.28.154",6379);
    }

    /**
     * 将数据转换为HbasePut 同时存入Redis缓存
     * @param data 账号数据
     * @return 账号数据HbasePut
     */
    @Override
    public Put map(String data) throws Exception {
        JSONObject dataJson = new JSONObject(data);
        Put put = new Put(dataJson.getString("eAccountId").getBytes());
        put.addColumn("f".getBytes(),"accountData".getBytes(),data.getBytes());
        Jedis jedis = jedisPool.getResource();
        jedis.set(dataJson.getString("eAccountId"),data);
        jedis.close();
        return put;
    }
}
