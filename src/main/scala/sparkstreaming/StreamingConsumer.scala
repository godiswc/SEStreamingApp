package sparkstreaming

import onecloud.plantpower.database.driver.protobuf.Driver.FindDataPointResponse
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.json4s.jackson.JsonMethods.{compact, parse}

/**
  * Created by root on 6/6/17.
  */
class StreamingConsumer extends Serializable{


  def startCompute(stream:org.apache.spark.streaming.dstream.DStream[(String,Array[Byte])],table_name:String): Unit = {
    stream.foreachRDD(rdd => {
      rdd.foreach { x =>
        println("Enter iteration")
        val response = this.parseFrom(x._2)
        //create HBase connection
        //val conf = HBaseConfiguration.create()

        val (conf,admin,table) = connect2hbase(table_name)
        val hbaseDAO = new HBaseDAO(conf)
        hbaseDAO.putData(response,table)
      }
    })
  }

  def parseFrom(x:Array[Byte]): FindDataPointResponse = {
    val response = FindDataPointResponse.parseFrom(x)
    response
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
    val conf = HBaseConfiguration.create()
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
