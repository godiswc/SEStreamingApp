package sparkstreaming

import java.io.FileInputStream
import java.util.Properties

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import com.cloudera.common.SEStreamingConstants
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */

object StreamingLauncher{

  def main(args: Array[String]) {
    if (args.length < 1) {
      //System.err.println("Usage: KafkaHBase <zkQuorum> <group> <topics> <table_name> <numThreads>")
      System.err.println("Usage: StreamingLuncher consumer.properites")
      System.exit(1)
    }
    val Array(propFile) = args
    val prop:java.util.Properties = new Properties();
    prop.load(new FileInputStream(propFile))

    val windowsSeconds = prop.getProperty(SEStreamingConstants.WINDOWSECONDS).toLong
    val sparkConf = new SparkConf().setAppName("HBase Streaming Ingestion")
    val ssc = new StreamingContext(sparkConf, Seconds(windowsSeconds))
    val consumer = new StreamingConsumer()
    ssc.checkpoint("/user/root/checkpoint")

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> prop.getProperty(SEStreamingConstants.SERVERS),
      "zookeeper.connect" -> prop.getProperty(SEStreamingConstants.ZOOKEEPERCONNECT),
      "group.id" -> "consumer",
      "auto.offset.reset" -> prop.getProperty(SEStreamingConstants.CONSUMEROFFSET)
    )

    val topics = prop.getProperty(SEStreamingConstants.CONSUMERTOPICS)
    val numThreads = prop.getProperty(SEStreamingConstants.CONSUMERNUMTHREADS)
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap


    val dps: DStream[(String, Array[Byte])] = KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK).window(Seconds(5), Seconds(5))
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

    consumer.startCompute(dps, table_name)
    ssc.start()
    ssc.awaitTermination()
  }
}
