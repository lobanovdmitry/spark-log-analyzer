package ru.dlobanov.loganalyzer

import java.sql.Timestamp

case class Alert(timestamp: Timestamp, host: String, error_rate: Double)
