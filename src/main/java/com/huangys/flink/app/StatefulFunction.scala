package com.huangys.flink.app

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object StatefulFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream:DataStream[(Int,Long)] = env.fromElements((2,21L),(4,1L),(5,4L),(2,123L),(4,123L))
    inputStream
      .keyBy(_._1)
      .map{
      new RichMapFunction[(Int,Long),(Int,Long,Long)] {
        private var leastValueState:ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val leastValueStateDescriptor = new ValueStateDescriptor[Long]("leastValue",classOf[Long],Long.MaxValue)
          leastValueState = getRuntimeContext.getState(leastValueStateDescriptor)
        }
        override def map(t: (Int, Long)): (Int,Long,Long) = {
          val leastValue = leastValueState.value()
          if(t._2 > leastValue) {
            (t._1,t._2,leastValue)
          }else {
            leastValueState.update(t._2)
            (t._1,t._2,t._2)
          }
        }
      }
    }.print()
    env.execute("Stateful")
  }
}
