package ru.dlobanov.loganalyzer

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object KafkaLogProducer extends App {

  implicit val formats: DefaultFormats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  }

  val levels = Array("TRACE", "DEBUG", "INFO", "WARN", "ERROR")
  val events = 10000
  val topic = "logs"
  val brokers = "localhost:9092"
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  for (nEvents <- Range(0, events)) {
    val messages = generateEvents(rnd.nextInt(10))
    val data = new ProducerRecord[String, String](topic, write(messages))

    println(write(messages))

    producer.send(data)
    Thread.sleep(rnd.nextInt(500))
  }

  producer.close()

  def generateEvents(n: Int) = {
    Range(0, n).map(i => generateEvent())
  }

  def generateEvent() = {
    val eventTime = new Timestamp(new Date().getTime)
    val host = "192.168.99." + (100 + rnd.nextInt(3))
    val level = levels(rnd.nextInt(levels.length))
    val msg = "New log message"
    LogMessage(eventTime, host, level, msg)
  }
}
