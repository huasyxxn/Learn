package com.huangys.flink.content

import java.time.ZoneId

import com.huangys.constant.Constant
import com.huangys.flink.content.map.Account2PutMapFunction
import com.huangys.flink.sink.HbaseSink
import com.huangys.flink.util.FlinkUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hbase.client.Put
import org.json.JSONObject
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object ZmtAccountRealTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    val flinkUtil = new FlinkUtil

    val accountStream = flinkUtil.getKafkaStream(Constant.KAFKA_BROKER_PETER,"ZmtAccountRealTime"
      ,"latest",Constant.ZMTACCOUNT,env)
//    val accountStream = env.fromElements("""{"eAccountId":"eAccountId1","accountData":"accountData1"}""")

//    val sink = new BucketingSink[String]("/user/prod/account")
//    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")))
//    sink.setBatchSize(1024 * 1024 * 400L)
//    sink.setBatchRolloverInterval(60* 60 * 1000L)
//    sink.setPendingPrefix("")
//    sink.setPendingSuffix("")
//    sink.setInProgressPrefix(".")
//    accountStream.addSink(sink)

    accountStream
      .map(new Account2PutMapFunction).print()
//      .addSink(new HbaseSink("10.80.10.159,10.80.7.160,10.80.1.160,10.80.30.159,10.80.32.160","zmtAccountInfo"))
//      .addSink(new HbaseSink("10.90.116.147,10.90.117.147,10.90.118.147,10.90.115.147,10.90.114.147","zmtAccountInfo"))

    env.execute("account")
  }

}
