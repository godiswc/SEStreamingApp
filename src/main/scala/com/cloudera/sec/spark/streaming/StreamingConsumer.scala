package com.cloudera.sec.spark.streaming

import onecloud.plantpower.database.driver.protobuf.Driver.FindDataPointResponse
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable

/**
  * Created by root on 6/6/17.
  */
class StreamingConsumer extends Serializable{

  def startCompute(stream:org.apache.spark.streaming.dstream.DStream[(String,Array[Byte])],table_name:String): Unit = {
    stream.foreachRDD(rdd => {
      rdd.foreach { x =>
        val response = this.parseFrom(x._2)
        println("enter iteration")
        val conf = HBaseConfiguration.create()
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
        val hbaseDAO = new HBaseDAO(conf)
        val table  = new HTable(conf,table_name)
        hbaseDAO.putData(response,table)
      }
    })
  }

  def parseFrom(x:Array[Byte]): FindDataPointResponse = {
    val response = FindDataPointResponse.parseFrom(x)
    response
  }
}
