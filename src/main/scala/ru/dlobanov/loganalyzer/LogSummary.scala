package ru.dlobanov.loganalyzer

import java.sql.Timestamp

case class LogSummary(timestamp: Timestamp, host: String, level: String, total: Long, rate: Double)
