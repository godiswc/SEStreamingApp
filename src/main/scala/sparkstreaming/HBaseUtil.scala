package sparkstreaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin

/**
  * Created by root on 6/7/17.
  */
class HBaseUtil {

  val conf: Configuration = HBaseConfiguration.create()
  val hAdmin = new HBaseAdmin(conf)

  def createHBaseConfig: Configuration ={
    val conf:Configuration = HBaseConfiguration.create()
    conf.addResource("/etc/hbase/conf/hbase-site.xml")
    conf
  }

  def isExist(tableName:String): Unit ={
    //val hAdmin:HBaseAdmin =new HBaseAdmin(conf)
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

  def createTable(tableName: String, columnFamilys: Array[String],splitKey: Array[Array[Byte]]): Unit = {
    //val hAdmin: HBaseAdmin = new HBaseAdmin(conf)
    if (hAdmin.tableExists(tableName)) {
      println("表" + tableName + "已经存在")
      return
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      hAdmin.createTable(tableDesc,splitKey)
      println("创建表成功")
    }
  }
}
