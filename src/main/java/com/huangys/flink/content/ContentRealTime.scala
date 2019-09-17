package com.huangys.flink.content

import com.huangys.flink.util.FlinkUtil
import com.huangys.constant.Constant
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.json.JSONObject
object ContentRealTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    val flinkUtil = new FlinkUtil


//    val ucmsStream = flinkUtil.getKafkaStream(Constant.KAFKA_BROKER_PETER,"ContentRealTime_ucms"
//      ,"latest",Constant.UCMSART_TOPIC,env)
    val ucmsStream = env.fromElements("{\"zmt\":\"2675\",\"creator\":\"weMedia\",\"copyright\":\"1\",\"Is_original_wemedia\":2,\"fhh\":\"6579390038860103686\",\"word_num\":13,\"sourceLink\":\"http://www.budejie.com/detail-29772000.html\",\"source\":\"百思不得姐\",\"title\":\"《家有儿女》确实是良心剧啊\",\"type\":\"article\",\"url\":\"https://feng.ifeng.com/c/7q1fvEgn1Wg\",\"wapUrl\":\"https://feng.ifeng.com/c/7q1fvEgn1Wg\",\"tm2\":\"2019-09-16 23:38:01\",\"id\":\"7q1fvEgn1Wg\",\"tm3\":\"2019-09-17 00:00:52\",\"page\":\"1\",\"dataProvider\":4,\"dataSource\":\"20005\",\"sourceFrom\":\"weMedia\",\"status\":1,\"timestamp\":1568649653982}")

    ucmsStream

    env.execute("ContentRealTime")
  }
}
