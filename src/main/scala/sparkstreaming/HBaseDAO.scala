package sparkstreaming

import java.util

import onecloud.plantpower.database.driver.protobuf.Driver.FindDataPointResponse
import onecloud.plantpower.database.driver.protobuf.TSDBStruct.DataPoint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by root on 6/6/17.
  */
class HBaseDAO(hbaseConf:Configuration){

  val cfbytes = Bytes.toBytes("cf")
  val qbytes =  Bytes.toBytes("v")


  def putData(response:FindDataPointResponse,table:HTable): Unit = {
    //val put = new Put(Bytes.toBytes(response.get))
    for (i <- 0 to (response.getDataPointsList.size()-1)) {
      val dataPoint = response.getDataPointsList.get(i)
      val list = buildPut(dataPoint)
      try {
        table.put(list)
      }catch{
        case e:Exception => println("exception caught: "+e)
          e.printStackTrace()
      }

    }
  }

  def buildPut(dataPoint:DataPoint): java.util.List[Put] ={
    //val put = new Put(B)
    val factoryCode =  dataPoint.getPoint.getPointGroup.getFactory.getCode
    val pointGroupCode = dataPoint.getPoint.getPointGroup.getCode
    val pointCode = dataPoint.getPoint.getCode
    //dataPoint.getPoint.getT
    val values = dataPoint.getValuesList
    val putList:java.util.List[Put] = new util.ArrayList[Put]()


    for (i <- 0 to (values.size()-1) ){
      val value = values.get(i)
      val timestamp = value.getTimestamp
      val rowkey = genRowkey(factoryCode,pointGroupCode,pointCode,timestamp);
      val put = new Put(rowkey)

      if(value.hasBoolValue)  put.add(cfbytes,qbytes,Bytes.toBytes(value.getBoolValue))
      if(value.hasDoubleValue) put.add(cfbytes,qbytes,Bytes.toBytes(value.getDoubleValue))
      if(value.hasFloatValue) put.add(cfbytes,qbytes,Bytes.toBytes(value.getFloatValue))
      if(value.hasIntValue) put.add(cfbytes,qbytes,Bytes.toBytes(value.getIntValue))
      if(value.hasLongValue) put.add(cfbytes,qbytes,Bytes.toBytes(value.getLongValue))

      println("=============Value======= "+ value.getFloatValue)
      putList.add(put)
    }
    putList
  }

  def genRowkey(factoryCode:String,pointGroupCode:String,pointCode:String,timeStamp:Long): Array[Byte] ={
    val row = pointCode+"|"+factoryCode+"|"+pointGroupCode+"|"+timeStamp
    println("======row ======   "+row)
    row.getBytes()
  }
}
