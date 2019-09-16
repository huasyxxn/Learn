package com.huangys.flink.app

import java.util.Properties

import com.huangys.constant.Constant
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, ProcessingTimeTrigger}
object WindowApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", Constant.KAFKA_BROKER)
    kafkaProps.setProperty("group.id", "NewUserFilter")
    kafkaProps.setProperty("auto.offset.reset", "latest")

    val errStream = env.addSource(new FlinkKafkaConsumer010[String](Constant.ERR_TOPIC, new SimpleStringSchema(),kafkaProps))

    val userKeyedStream = errStream.map(log=>{
      (log.split("userkey=")(1).split(" ")(0).split("&")(0),1)
    }).keyBy(0)

    userKeyedStream.timeWindow(Time.seconds(10))
      .trigger(EventTimeTrigger.create())//通过对比Watermark和窗口EndTime确定是否触发窗口
      .reduce(new MyReduceFunction)
      .print()


    env.execute("windowApp")
  }
  class MyReduceFunction extends ReduceFunction[(String,Int)]{
    override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
      (t._1,t1._2+t._2)
    }
  }
}
