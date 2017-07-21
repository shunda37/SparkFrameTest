package manager

import java.io.File

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.util.Try

/**
  * Created by Administrator on 2017/7/14.
  */
object SparkManager extends SLF4JLogging{
  private var sc : Option[SparkContext] = None
  private var sqlContext: Option[SQLContext] = None
  private var ssc: Option[StreamingContext] = None

  def sparkSqlContextInstance: Option[SQLContext] = {
    synchronized{
      sqlContext match {
        case Some(_) => sqlContext
        case None => if(sc.isDefined) sqlContext = Some(SQLContext.getOrCreate(sc.get))
      }
    }
    sqlContext
  }

  def sparkInstance: Option[SparkContext] = sc
  def sparkStreamingInstance: Option[StreamingContext] = ssc

  def sparkStreamingInstance(batchDuration: Duration,checkpointDir:String): Option[StreamingContext] = {
    synchronized{
      ssc match {
        case Some(_) => ssc
        case None => ssc = Some(getNewStreamingContext(batchDuration,checkpointDir))
      }
    }
    ssc
  }

  def setSparkContext(createdContext: SparkContext): Unit = sc = Option(createdContext)
  def setSparkStreamingContext(createdContext: StreamingContext):Unit = ssc = Option(createdContext)

  private def getNewStreamingContext(batchDuration: Duration,checkpointDir: String): StreamingContext ={
    val ssc = new StreamingContext(sc.get, batchDuration)
    ssc
  }

  def sparkStandAloneContextInstance(generalConfig : Option[Map[String,String]],
                                     specificConfig:Map[String,String],
                                    jars: Seq[File]):SparkContext = {
    synchronized{
      sc.getOrElse(initStandAloneContext(generalConfig,specificConfig,jars))
    }
  }

  def sparkClusterContextInstance(specificConfig:Map[String,String],jars: Seq[String] = Seq[String]()):SparkContext ={
    synchronized{
      sc.getOrElse(initClusterContext(specificConfig,jars))
    }
  }

  private def initStandAloneContext(generalConfig : Option[Map[String,String]],
                                    specificConfig:Map[String,String],
                                    jars: Seq[File]):SparkContext = {
    sc = Some(SparkContext.getOrCreate(mapToSparkConf(generalConfig,specificConfig)))
    jars.foreach(f => sc.get.addJar(f.getAbsolutePath))
    sc.get
  }

  private def initClusterContext(specificConfig:Map[String,String],
                                jars: Seq[String] = Seq[String]()):SparkContext ={
    sc = Some(SparkContext.getOrCreate(mapToSparkConf(None,specificConfig)))
    jars.foreach(f => sc.get.addJar(f))
    sc.get
  }

  private def mapToSparkConf(generalConfig: Option[Map[String,String]],specificConfig:Map[String,String]):SparkConf ={
    val conf = new SparkConf()
    if(generalConfig.isDefined){
      generalConfig.get.foreach{
        case (key,value) => {conf.set(key, value)}
      }
    }
    specificConfig.foreach{case (key, value) =>conf.set(key,value) }
    conf
  }

  def destroySparkStreamingContext(): Unit ={
    synchronized{
      ssc.fold(log.warn("Spark Streaming Context is Empty")){streamingContext =>
        try{
          val stopGracefully = true
          val stopTimeout = 3
          log.info(s"Stopping streamingContext with name: ${streamingContext.sparkContext.appName}")
          Try(streamingContext.stop(false, stopGracefully)).getOrElse(streamingContext.stop(false,false))
          if (streamingContext.awaitTerminationOrTimeout(stopTimeout))
            log.info(s"Stopped streamingContext with name: ${streamingContext.sparkContext.appName}")
          else log.info(s"StreamingContext with name: ${streamingContext.sparkContext.appName} not Stopped")
        }finally{
          ssc = None
        }
      }
    }
  }
  def destroySparkContext(): Unit ={
    synchronized{
      destroySparkStreamingContext()
      sc.fold(log.warn("Spark Context is Empty")){sparkContext =>
        try{
          log.info("Stopping SparkContext with name: "+sparkContext.appName)
          sparkContext.stop()
          log.info("Stopping SparkContext with name: "+sparkContext.appName)
        }finally{
          sc = None
        }
      }
    }
  }

}
