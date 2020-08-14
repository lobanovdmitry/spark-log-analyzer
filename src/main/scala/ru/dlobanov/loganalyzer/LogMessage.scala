package ru.dlobanov.loganalyzer

import java.sql.Timestamp

case class LogMessage(timestamp: Timestamp, host: String, level: String, text: String)
