package com.huangys.flink.content.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.json.JSONObject;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Set;

public class Account2PutMapFunction extends RichMapFunction<String,Put> {
    private JedisCluster jedisCluster;

    @Override
    public void open(Configuration config){
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("10.80.28.154",6379));
        nodes.add(new HostAndPort("10.80.29.154",6379));
        nodes.add(new HostAndPort("10.80.30.154",6379));
        nodes.add(new HostAndPort("10.80.31.154",6379));
        nodes.add(new HostAndPort("10.80.32.154",6379));
        jedisCluster = new JedisCluster(nodes);
    }

    /**
     * 将数据转换为HbasePut 同时存入Redis缓存
     * @param data 账号数据
     * @return 账号数据HbasePut
     */
    @Override
    public Put map(String data) throws Exception {
        JSONObject dataJson = new JSONObject(data);
        Put put = new Put(Integer.toString(dataJson.getInt("eAccountId")).getBytes());
        put.addColumn("f".getBytes(),"accountData".getBytes(),data.getBytes());
        jedisCluster.set("account_"+Integer.toString(dataJson.getInt("eAccountId")),data);
        return put;
    }

    @Override
    public void close(){
        jedisCluster.close();
    }
}
