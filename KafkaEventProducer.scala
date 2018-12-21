package org.training.spark.streaming

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.training.spark.util.KafkaRedisProperties

import scala.util.Random

object KafkaEventProducer {

  private val users = Array(
    "user1", "user2",
    "user3", "user4",
    "user5", "user6",
    "user7", "user8",
    "user9", "user10")

  private val random = new Random()

  private var pointer = -1

  def getUserID() : String = {
    pointer = pointer + 1
    if(pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }

  def click() : Double = {
    random.nextInt(10)
  }

  def main(args: Array[String]): Unit = {
    val topic = KafkaRedisProperties.KAFKA_USER_TOPIC
    val brokers = KafkaRedisProperties.KAFKA_ADDR
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaRedisProperties.KAFKA_ADDR)
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      // prepare event data
      val event = new JSONObject()
      event.put("uid", getUserID)
      event.put("event_time", System.currentTimeMillis.toString)
      event.put("os_type", "Android")
      event.put("click_count", click)

      producer.send(new ProducerRecord[String,String](topic, event.toString) )
      println("Message sent: " + event)

      Thread.sleep(1000)
    }
  }
}
