package ru.dlobanov.loganalyzer

import org.apache.log4j.Logger
import org.apache.spark.sql.ForeachWriter

class LogWriter extends ForeachWriter[String] with Serializable {

  @transient
  private lazy val logger = Logger.getLogger(this.getClass)

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: String): Unit = logger.debug(value)

  override def close(errorOrNull: Throwable): Unit = {}
}

