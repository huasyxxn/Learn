package com.huangys.flink.content

import com.huangys.constant.Constant
import com.huangys.flink.content.map.Account2PutMapFunction
import com.huangys.flink.sink.HbaseSink
import com.huangys.flink.util.FlinkUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hbase.client.Put
import org.json.JSONObject

object ZmtAccountRealTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    val flinkUtil = new FlinkUtil

    val accountStream = flinkUtil.getKafkaStream(Constant.KAFKA_BROKER_PETER,"ZmtAccountRealTime"
      ,"latest",Constant.ZMTACCOUNT,env)

    accountStream.map(new Account2PutMapFunction)
      .addSink(new HbaseSink("10.90.116.147,10.90.117.147,10.90.118.147,10.90.115.147,10.90.114.147","zmtAccountInfo"))

    env.execute("account")
  }

}
