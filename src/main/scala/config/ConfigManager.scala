package config

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConversions._
import util.DataUtil

/**
  * Created by Administrator on 2017/7/14.
  */
object ConfigManager {
  var mConfPath = ""
  var mPath = ""
  val mProp : Properties = new Properties()


  def setConfPath(iConfPath: String): Unit ={
    mPath = iConfPath+"/application.properties"
    getProp(mPath)
  }

  private def getProp(mConfPath: String) ={
    mProp.load(new FileInputStream(mConfPath))
  }

  def getSparkConf():Map[String ,String] = {
    DataUtil.getPropsByPrefix(mProp,"sparkconf.").toMap
  }
  def getSparkContextLogLevel() : String = {
    mProp.getProperty("sparkcontext.LogLevel","WARN")
  }

  def getKafkaConf() : Properties = {
    val kafkaPro : Properties = new Properties()
    kafkaPro.load(new FileInputStream(mConfPath+"/kafkaProducer.properties"))
    kafkaPro
  }


  def getAkkaConf() :(String,String,Int) = {
    val system : String = mProp.getProperty("akka.system","")
    val masterIp : String = mProp.getProperty("akka.master.ip","")
    val masterPort : Int = mProp.getProperty("akka.master.port","0").toInt
    (system,masterIp,masterPort)
  }

  def getPargPath():String = {
    val pargPath : String = mProp.getProperty("parq.path")
    pargPath
  }

}
