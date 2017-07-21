package main

import config.ConfigManager
import kafka.KafkaProducerUntil
import manager.{AppInit, SparkManager}
import org.apache.log4j.Logger

/**
  * Created by Administrator on 2017/7/14.
  */
object Main extends Serializable{
  val logger = Logger.getLogger(Main.getClass)
  def main(args : Array[String]): Unit ={
    AppInit.initApp()
    logger.info("配置文件读取完毕 ")
    SparkManager.sparkClusterContextInstance(ConfigManager.getSparkConf())
//    val ssi = SparkManager.sparkSqlContextInstance
//    val users = ssi match {
//      case Some(sqlContext) => sqlContext.read.load(ConfigManager.getPargPath())
//    }
    try{
      val sqlContext = SparkManager.sparkSqlContextInstance.get
      val users = sqlContext.read.load(ConfigManager.getPargPath())
      println("ssssssssssssssssssssssss = "+users.count())
      users.show(10)

      KafkaProducerUntil.send("Abner","123456")

      val list = List("Abner")
      KafkaProducerUntil.receive(list)
    }catch {
      case e : Exception => e.printStackTrace()
    }

  }

}
