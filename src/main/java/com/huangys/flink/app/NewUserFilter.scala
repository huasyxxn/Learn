package com.huangys.flink.app

import java.util.Properties

import com.huangys.constant.Constant
import com.huangys.flink.map.{AppErrAnalysisMapFunction, UserPreLogMapFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
object NewUserFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", Constant.KAFKA_BROKER)
    kafkaProps.setProperty("group.id", "NewUserFilter")
    kafkaProps.setProperty("auto.offset.reset", "latest")

    val errStream = env.addSource(new FlinkKafkaConsumer010[String](Constant.ERR_TOPIC, new SimpleStringSchema(),kafkaProps))
    //errStream.setParallelism(1).map(new AppErrAnalysisMapFunction).print()
    errStream.map(log=>{
      (log.split("userkey=")(1).split("&")(0).split(" ")(0),log)
    }).keyBy(0).map(new UserPreLogMapFunction).print()

    env.execute("NewUserFilter")
  }
}
