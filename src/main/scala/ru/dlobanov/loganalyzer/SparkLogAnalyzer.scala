package ru.dlobanov.loganalyzer

import java.text.SimpleDateFormat

import net.liftweb.json.Serialization.write
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class SparkLogAnalyzer(val windowDurationSec: Int, val slideDurationSec: Int) extends Serializable {

  import SparkLogAnalyzer._

  def setupProcessing(events: Dataset[String])(implicit spark: SparkSession): (DataStreamWriter[String], DataStreamWriter[String]) = {
    import spark.implicits._

    val windowedCounts = events
      .flatMap(asLogMessages)
      .withWatermark("timestamp", s"$WatermarkDelay seconds")
      .groupBy(window($"timestamp", s"$windowDurationSec seconds", s"$slideDurationSec seconds"), $"host", $"level")
      .count()
      .map(asLogSummary(windowDurationSec))

    val logSummary = windowedCounts
      .map(toJson)
      .writeStream
      .outputMode(OutputMode.Append())

    val alerts = windowedCounts
      .filter("ERROR" == _.level)
      .filter(_.rate >= 1.0)
      .map(summary => Alert(summary.timestamp, summary.host, summary.rate))
      .map(toJson)
      .writeStream
      .outputMode(OutputMode.Append())

    (logSummary, alerts)
  }
}

object SparkLogAnalyzer {

  private val WatermarkDelay = 0

  implicit val dateFormat: DefaultFormats = new DefaultFormats {
    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  }

  def asLogMessages(content: String): List[LogMessage] = {
    parse(content).extractOrElse[List[LogMessage]](List.empty)
  }

  def asLogSummary(window: Int)(row: Row): LogSummary = {
    LogSummary(
      timestamp = row.getAs[Row](0).getTimestamp(1),
      host = row.getAs[String](1),
      level = row.getAs[String](2),
      total = row.getAs[Long](3),
      rate = row.getAs[Long](3) / window.toDouble
    )
  }

  def toJson(obj: Any): String = {
    write(obj)
  }
}
