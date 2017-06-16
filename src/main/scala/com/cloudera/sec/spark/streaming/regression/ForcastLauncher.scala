package com.cloudera.sec.spark.streaming.regression

import java.io.FileInputStream
import java.util.Properties

import com.cloudera.common.SEStreamingConstants
import com.cloudera.sec.spark.streaming.HBaseUtil
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 6/12/17.
  */
object ForcastLauncher {

  def main(args: Array[String]): Unit ={

    if (args.length < 1) {
      System.err.println("Usage: ForcashLuncher consumer.properites")
      System.exit(1)
    }
    val Array(propFile) = args
    val prop:java.util.Properties = new Properties();
    prop.load(new FileInputStream(propFile))

    val windowsSeconds = prop.getProperty(SEStreamingConstants.WINDOWSECONDS).toLong
    val sparkConf = new SparkConf().setAppName("Streaming Linear Regression")
    val ssc = new StreamingContext(sparkConf, Seconds(windowsSeconds))
    //val consumer = new StreamingConsumer()
    ssc.checkpoint("/user/root/checkpoint")

    val trainParams = Map[String, String](
      "bootstrap.servers" -> prop.getProperty(SEStreamingConstants.SERVERS),
      "zookeeper.connect" -> prop.getProperty(SEStreamingConstants.ZOOKEEPERCONNECT),
      "group.id" -> "consumer",
      "auto.offset.reset" -> "smallest"
    )

    val verifyParams = Map[String, String](
      "bootstrap.servers" -> prop.getProperty(SEStreamingConstants.SERVERS),
      "zookeeper.connect" -> prop.getProperty(SEStreamingConstants.ZOOKEEPERCONNECT),
      "group.id" -> "consumer1",
      "auto.offset.reset" -> "largest"
    )


    val table_name = prop.getProperty(SEStreamingConstants.HBASETABLE)
    val splitKey= prop.getProperty(SEStreamingConstants.HBASESPLITKEYS)
    val hbaseCF = prop.getProperty(SEStreamingConstants.HBASECF)
    val util = new HBaseUtil()
    val splitStr =  splitKey.split("\\|")
    val splitByte = new Array[Array[Byte]](splitStr.length)
    for(i <- 0 to splitStr.length-1){
      splitByte(i) = Bytes.toBytes(splitStr(i))
    }

    util.createTable(table_name,hbaseCF.split("\\|"),splitByte)

    val topics = prop.getProperty(SEStreamingConstants.CONSUMERTOPICS)
    val numThreads = prop.getProperty(SEStreamingConstants.CONSUMERNUMTHREADS)
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap


    val train: DStream[(String, Array[Byte])] = KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, trainParams, topicMap, StorageLevel.MEMORY_AND_DISK).window(Seconds(5), Seconds(5))


    val verify: DStream[(String, Array[Byte])] = KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, verifyParams, topicMap, StorageLevel.MEMORY_AND_DISK).window(Seconds(5), Seconds(5))
    //KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK)

    val trainer = new SECTrainer()
    trainer.train(train,verify,prop)
    ssc.start()
    ssc.awaitTermination()
  }
}
