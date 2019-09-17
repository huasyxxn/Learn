package com.huangys.flink.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._

class FlinkUtil {
  def getKafkaStream(broker:String,groupId:String,offsetSet:String,topic:String,env:StreamExecutionEnvironment): DataStream[String] ={
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", broker)
    kafkaProps.setProperty("group.id", groupId)
    kafkaProps.setProperty("auto.offset.reset", offsetSet)
    env.addSource(new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(),kafkaProps))
  }
}
