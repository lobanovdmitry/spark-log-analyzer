package ru.dlobanov.loganalyzer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamWriter

object Main {

  private val windowDurationInSecs = 60
  private val slideDurationInSecs = 10
  private val alertsTopic = "alerts"

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: SparkLogAnalyzer <broker.list> <input.topic> <sink.topic>
           |  <broker.list>     is a comma-separated list of host and port pairs that are the addresses of the Kafka brokers
           |  <input.topic>     is a kafka topic to consume logs from
           |  <sink.topic>      is a kafka topic to write logs stats info
            """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, inputTopic, sinkTopic) = args

    implicit val spark = SparkSession.builder
      .appName("SparkLogAnalyzer")
      //      .master("local[2]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    implicit val kafkaBrokers: String = brokers

    val events = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", inputTopic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val analyzer = new SparkLogAnalyzer(windowDurationInSecs, slideDurationInSecs)
    val (summary, alerts) = analyzer.setupProcessing(events)

    writeToKafka(summary, sinkTopic)
    writeToLogs(summary)

    writeToKafka(alerts, alertsTopic)
    writeToLogs(alerts)

    spark.streams.awaitAnyTermination()
  }

  private def writeToKafka(stream: DataStreamWriter[String], topic: String)(implicit kafkaBrokers: String) = {
    stream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", topic)
      .start()
  }

  private def writeToLogs(stream: DataStreamWriter[String]) = {
    stream
      .foreach(new LogWriter)
      .start()
  }
}
