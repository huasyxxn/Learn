package com.huangys.flink.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple3;


/**
 * @author huangys
 */
public class AppErrAnalysisMapFunction extends RichMapFunction<String, Tuple3<String,String,String>> {
    private String preUserkey = "#";
    private JedisPool jedisPool;

    @Override
    public void open(Configuration config){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPool = new JedisPool(jedisPoolConfig,"127.0.0.1",6379);
    }

    /**
     *
     * @param log 日志
     * @return 当前用户id,初始前一个用户id，当前前一个用户id
     */
    @Override
    public Tuple3<String, String,String> map(String log) throws Exception {
        String userkey = "#";
        String[] userkeySplit = log.split("userkey=");
        if(userkeySplit.length == 2){
            userkey = userkeySplit[1].split("&")[0].split(" ")[0];
        }
        String preUserkeyTmp = preUserkey;
        String preUserkeyTmpCurrent = preUserkey;
        preUserkey = userkey;

        Jedis jedis = jedisPool.getResource();
        jedis.select(1);
        if(jedis.exists("getPre_"+userkey)){
            preUserkeyTmp = jedis.get("getPre_"+userkey);
        }else{
            jedis.set("getPre_"+userkey, preUserkeyTmp);
        }
        jedis.sadd("getNext_"+preUserkeyTmp,userkey);
        jedis.close();
        return new Tuple3<>(userkey,preUserkeyTmp,preUserkeyTmpCurrent);
    }
}
