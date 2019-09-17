package com.huangys.flink.sink

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}

class HbaseSink(hbaseZookeeperHost:String,tableName:String,batchSize:Int = 0,batchNum:Int = -1) extends RichSinkFunction[Put]{
  var table:HTable = _
  var putList:util.ArrayList[Put] = _

  override def open(parameters: Configuration): Unit = {
    try {
      super.open(parameters)
      val configuration = HBaseConfiguration.create()
      configuration.set("hbase.zookeeper.quorum", hbaseZookeeperHost)
      configuration.set("hbase.zookeeper.property.clientPort", "2181")
      table = new HTable(configuration, tableName)
      if (batchSize > 0) {
        table.setAutoFlush(false, false)
        table.setWriteBufferSize(batchSize)
      }
      putList = new util.ArrayList[Put]()
      println("init")
    }catch {
      case e:Exception=>e.printStackTrace()
    }
  }

  override def invoke(put: Put, context: SinkFunction.Context[_]): Unit = {
    try{
      //      println("invoke")
      if(batchNum < 0 && put != null){
        //        println(new String(put.getRow))
        table.put(put)
      }else {
        putList.add(put)
        if(putList.size()>batchNum){
          table.put(putList)
        }
      }
    }catch {
      case e:Exception => //e.printStackTrace()
    }
  }

  override def close(): Unit = {
    table.flushCommits()
    table.close()
  }
}

