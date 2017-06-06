package sparkstreaming
import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get,Scan,ResultScanner}
import org.apache.hadoop.hbase.filter.{Filter,PrefixFilter,RowFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
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

object TestConsumer1{
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaHBase <zkQuorum> <group> <topics> <table_name> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, table_name, numThreads) = args
    val sparkConf = new SparkConf().setAppName("HBase Sample")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("/user/spark/checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val Json = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)

      .window(Seconds(5), Seconds(5))
      .map(_._2)

    //startCompute(Json,table_name)

    //test
    val words = Json.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()

    /******************************The following code is the implementations for functions*****************************/
    //computation starts here
    def startCompute(stream:org.apache.spark.streaming.dstream.DStream[String],table_name:String) = {
      Json.foreachRDD(rdd => {
        rdd.foreach { x =>
          //the data is in JSON so pass it to function_parse function to get extracted data
          val (stockSymbol,stockNumber,mP,bP,aP,bQ,aQ,vol,rowid) = function_parse(x)
          //create HBase connection
          val (conf,admin,table) = connect2hbase(table_name)
          //get a list of rows for the row key "stockNumber" and an iterator to go through them
          val iterator = getRowsByKey(stockNumber,table)
          /*
  * since the list will be in descending order, we will just use the first row and extract
  * required values to be used to update incoming data
  */
          if(iterator.hasNext()){
            val (oldMp,oldBp,oldAp) = getOldValuesByColumn(iterator.next())
            //calculate new values to update the running total based on the old values
            val (updated_mp,updated_bp,updated_ap) = calculateNewValues(mP,bP,aP,oldMp,oldBp,oldAp)
            //insert the data
            insert_data(stockSymbol,stockNumber,updated_mp,updated_bp,updated_ap,bQ,aQ,vol,rowid,table)
          }
          else {
            //insert the data
            insert_data(stockSymbol,stockNumber,mP,bP,aP,bQ,aQ,vol,rowid,table)
          }
        }
      })
    }

    def function_parse(x:String) = {
      val data = parse(x,false)
      val StockSymbol = compact(data \\ "StockSymbol")
      val StockSymbolF = StockSymbol.replace('"', ' ').trim()
      val StockNumber = compact(data \\ "StockNumber")
      val StockNumberF = StockNumber.replace('"', ' ').trim()
      val Mp = compact(data \\ "Mp")
      val MpF = Mp.replace('$', ' ').replace('\"', ' ').trim()
      val Bp = compact(data \\ "Bp")
      val BpF = Bp.replace('$', ' ').replace('\"', ' ').trim()
      val Ap = compact(data \\ "Ap")
      val ApF = Ap.replace('$', ' ').replace('\"', ' ').trim()
      val BQ = compact(data \\ "BQ")
      val BQF = BQ.replace('\"', ' ').trim()
      val Aq = compact(data \\ "Aq")
      val AqF = Aq.replace('\"', ' ').trim()
      val Vol = compact(data \\ "Vol")
      val VolF = Vol.replace('\"', ' ').trim()
      val rowId = compact(data \\ "rowId")
      val rowIdF = rowId.replace('\"', ' ').trim()
      //sending back the formatted content of the JSON string
      (StockSymbolF,StockNumberF,MpF,BpF,ApF,BQF,AqF,VolF,rowIdF)
    }
    def connect2hbase(table_name:String) = {
      val conf = new HBaseConfiguration()
      val admin = new HBaseAdmin(conf)
      val table = new HTable(conf, table_name)
      (conf,admin,table)
    }
    def getRowsByKey(rowkey:String,table:HTable) = {
      val s = new Scan().setReversed(true)
      val filter = new PrefixFilter(Bytes.toBytes(rowkey+"_"))
      s.setFilter(filter)
      s.addFamily(Bytes.toBytes("mycf"))
      val rs = table.getScanner(s)
      (rs.iterator())
    }
    def getOldValuesByColumn(nxt:org.apache.hadoop.hbase.client.Result) = {
      val oldMp = Bytes.toString(nxt.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("col_Mp")))
      val oldBp = Bytes.toString(nxt.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("col_Bp")))
      val oldAp = Bytes.toString(nxt.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("col_Ap")))
      (oldMp,oldBp,oldAp)
    }

    def calculateNewValues(c:String,d:String,e:String,oldMp:String,oldBp:String,oldAp:String) = {
      val NewMp = (c.toDouble + oldMp.replace('\"', ' ').trim().toDouble).toString()
      val NewBp = (d.toDouble + oldBp.replace('\"', ' ').trim().toDouble).toString()
      val NewAp = (e.toDouble + oldAp.replace('\"', ' ').trim().toDouble).toString()
      (NewMp,NewBp,NewAp)
    }
    def insert_data(StockSymbol:String,StockNumber:String,Mp:String,Bp:String,Ap:String,BQ:String,Aq:String,Vol:String,rowId:String,table:HTable) = {
      val ts = System.currentTimeMillis()
      val theput = new Put(Bytes.toBytes(StockNumber+"_"+ts.toString()),ts)
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_SS"),Bytes.toBytes(StockSymbol))
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_Mp"),Bytes.toBytes(Mp))
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_Bp"),Bytes.toBytes(Bp))
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_Ap"),Bytes.toBytes(Ap))
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_BQ"),Bytes.toBytes(BQ))
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_Aq"),Bytes.toBytes(Aq))
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_Vol"),Bytes.toBytes(Vol))
      theput.add(Bytes.toBytes("mycf"),Bytes.toBytes("col_rowId"),Bytes.toBytes(rowId))
      table.put(theput)
    }
  }
}
