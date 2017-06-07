package sparkstreaming

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
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

/**
object DirectKafka {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafka <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

   val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
  **/

object StreamingLauncher{


  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaHBase <zkQuorum> <group> <topics> <table_name> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, table_name, numThreads) = args
    val sparkConf = new SparkConf().setAppName("HBase Streaming Ingestion")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val consumer = new StreamingConsumer()
    //ssc.checkpoint("/user/root/checkpoint")

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "10.84.3.21:9092,10.84.3.22:9092,10.84.3.23:9092,10.84.3.24:9092,10.84.3.25:9092,10.84.3.26:9092,10.84.3.28:9092",
      //"key.deserializer" -> classOf[StringDeserializer],
      //"value.deserializer" -> classOf[ByteArrayDeserializer],
      "zookeeper.connect" -> "manager.serc.com:2181,master1.serc.com:2181,master2.serc.com:2181",
      "group.id" -> "kafka",
      "auto.offset.reset" -> "largest"
      //"enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //    val Json = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    //      .window(Seconds(5), Seconds(5))
    //      .map(_._2)

    val dps: DStream[(String, Array[Byte])] = KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK).window(Seconds(5), Seconds(5))

    consumer.startCompute(dps, table_name)
    ssc.start()
    ssc.awaitTermination()

    /** ****************************The following code is the implementations for functions *****************************/
    //computation starts here
    //    def startCompute(stream:org.apache.spark.streaming.dstream.DStream[String],table_name:String) = {
    //      Json.foreachRDD(rdd => {
    //        rdd.foreach { x =>
    //          //the data is in JSON so pass it to function_parse function to get extracted data
    //          val (stockSymbol,stockNumber,mP,bP,aP,bQ,aQ,vol,rowid) = function_parse(x)
    //          //create HBase connection
    //          val (conf,admin,table) = connect2hbase(table_name)
    //          //get a list of rows for the row key "stockNumber" and an iterator to go through them
    //          val iterator = getRowsByKey(stockNumber,table)
    //          /*
    //  * since the list will be in descending order, we will just use the first row and extract
    //  * required values to be used to update incoming data
    //  */
    //          if(iterator.hasNext()){
    //            val (oldMp,oldBp,oldAp) = getOldValuesByColumn(iterator.next())
    //            //calculate new values to update the running total based on the old values
    //            val (updated_mp,updated_bp,updated_ap) = calculateNewValues(mP,bP,aP,oldMp,oldBp,oldAp)
    //            //insert the data
    //            insert_data(stockSymbol,stockNumber,updated_mp,updated_bp,updated_ap,bQ,aQ,vol,rowid,table)
    //          }
    //          else {
    //            //insert the data
    //            insert_data(stockSymbol,stockNumber,mP,bP,aP,bQ,aQ,vol,rowid,table)
    //          }
    //        }
    //      })
    //
  }
}
