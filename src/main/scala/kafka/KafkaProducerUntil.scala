package kafka

import akka.io.Tcp.Message
import manager.KafkaManager
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2017/7/21.
  */
object KafkaProducerUntil {
  private val logger = Logger.getLogger(KafkaProducerUntil.getClass)

  def send(topic : String ,message: String): Unit ={
    val mKafkaProducer = KafkaManager.getmKafkaProducer
    val record = new ProducerRecord[String , String](topic,"",message)
    mKafkaProducer.send(record)
  }

  def receive(topics : List[String]): Unit ={
    val mKafkaConsumer = KafkaManager.getmKafkaConsumer
    mKafkaConsumer.subscribe(topics)

    val records : ConsumerRecords[String, String] = mKafkaConsumer.poll(200)
    for(record <- records : ConsumerRecords[String,String]){
      logger.info("1111111111111111111111"+record)
    }
  }

}
