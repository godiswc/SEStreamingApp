package sparkstreaming

import java.util

import onecloud.plantpower.database.driver.protobuf.Driver.FindDataPointResponse
import onecloud.plantpower.database.driver.protobuf.TSDBStruct.DataPoint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}

/**
  * Created by root on 6/6/17.
  */
class HBaseDAO(hbaseConf:Configuration){

  val conf: Configuration = hbaseConf
  val cfbytes = Bytes.toBytes("cf")
  val qbytes =  Bytes.toBytes("v")

  def createHBaseConfig: Unit ={
    val conf:Configuration = HBaseConfiguration.create()
    conf.addResource("/etc/hbase/conf/hbase-site.xml")
    conf
  }

  def isExist(tableName:String): Unit ={
    val hAdmin:HBaseAdmin =new HBaseAdmin(conf)
    hAdmin.tableExists(tableName)
  }

  def createTable(tableName: String, columnFamilys: Array[String]): Unit = {
    val hAdmin: HBaseAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(tableName)) {
      println("表" + tableName + "已经存在")
      return
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      hAdmin.createTable(tableDesc)
      println("创建表成功")
    }
  }

  def putData(response:FindDataPointResponse,table:HTable): Unit = {
    //val put = new Put(Bytes.toBytes(response.get))
    for (i <- 0 to (response.getDataPointsList.size()-1)) {
      val dataPoint = response.getDataPointsList.get(i)
      val list = buildPut(dataPoint)
      //table.put(list)
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
      put.add(cfbytes,qbytes,Bytes.toBytes(value.getFloatValue))
      println("Value "+ value.getFloatValue)
      putList.add(put)
    }
    putList
  }

  def genRowkey(factoryCode:String,pointGroupCode:String,pointCode:String,timeStamp:Long): Array[Byte] ={
    val row = factoryCode+"|"+pointGroupCode+"|"+pointCode+"|"+timeStamp
    println("======row ======   "+row)
    row.getBytes()
  }
}
