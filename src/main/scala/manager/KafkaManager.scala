package manager

import java.util.Properties

import config.ConfigManager
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger

/**
  * Created by Administrator on 2017/7/21.
  */
object KafkaManager {
  //获取kafkaProducer对象
  private val mKafkaProducer : KafkaProducer[String,String] = setKafkaManager
  //获取kafkaConsumer对象
  private val mKafkaConsumer : KafkaConsumer[String,String] = setKafkaConsumer

  private val logger = Logger.getLogger(KafkaManager.getClass)

  def getmKafkaProducer : KafkaProducer[String , String ] = mKafkaProducer

  def getmKafkaConsumer : KafkaConsumer[String , String] = mKafkaConsumer

  def setKafkaManager : KafkaProducer[String,String]= {
    logger.info(" Start init KafkaManager ")
    val props = ConfigManager.getKafkaConf()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    new KafkaProducer[String,String](props)
  }
  Runtime.getRuntime.addShutdownHook(new Thread(){
    override def run(): Unit ={
      mKafkaProducer.close()
      mKafkaConsumer.close()
    }
  })

  def setKafkaConsumer :KafkaConsumer[String , String] = {
  val props = new Properties()
    props.put("bootstrap.service","")
    props.put("bootstrap.service","192.4.11.1:9092")
    props.put("group.id","1")
    props.put("enable.auto.commit","true")
    props.put("session.timeout.ms","30000")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.put("auto.offset.reset","earliest")
    props.put("max.partition.fetch.bytes","5120")
    new KafkaConsumer[String , String](props)
  }

}
