package ru.dlobanov.loganalyzer

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{RowFactory, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SparkLogAnalyzerSpec extends FlatSpec with BeforeAndAfter with Matchers {

  private val testWindowDuration = 5
  private val testSlideDuration = 5

  def withSpark(testCode: SparkSession => Any): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName(suiteName)
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.app.id", suiteId)
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      testCode(spark)
    } finally {
      spark.stop()
    }
  }

  "SparkLogAnalyzer" should "be able to compute average rate and total number of log messages by window" in withSpark { implicit spark =>
    import spark.implicits._

    // Prepare
    val memoryStream = new MemoryStream[String](1, spark.sqlContext)
    memoryStream.addData(
      """
        [
          {"timestamp":"2018-04-02T07:30:15.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:21.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:22.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:25.100Z","host":"host-1","level":"ERROR","text":"Error description"}
        ]"""
    )
    val events = memoryStream.toDS()

    // Business methods
    val analyzer = new SparkLogAnalyzer(testWindowDuration, testSlideDuration)
    val (summary, alerts) = analyzer.setupProcessing(events)
    val (summaryRows, alertsRows) = processData(summary, alerts)

    // Asserts
    summaryRows should have length 3
    alertsRows shouldBe empty

    summaryRows should contain allElementsOf Seq(
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-1","level":"ERROR","total":1,"rate":0.2}"""),
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:25.000Z","host":"host-1","level":"ERROR","total":2,"rate":0.4}"""),
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:30.000Z","host":"host-1","level":"ERROR","total":1,"rate":0.2}""")
    )
  }

  it should "be able to compute average rate and total number of log messages per host" in withSpark { implicit spark =>
    import spark.implicits._

    // Prepare
    val memoryStream = new MemoryStream[String](1, spark.sqlContext)
    memoryStream.addData(
      """
        [
          {"timestamp":"2018-04-02T07:30:15.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:16.100Z","host":"host-2","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:17.100Z","host":"host-2","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:18.100Z","host":"host-3","level":"ERROR","text":"Error description"}
        ]"""
    )
    val events = memoryStream.toDS()

    // Business methods
    val analyzer = new SparkLogAnalyzer(testWindowDuration, testSlideDuration)
    val (summary, alerts) = analyzer.setupProcessing(events)
    val (summaryRows, alertsRows) = processData(summary, alerts)

    // Asserts
    summaryRows should have length 3
    alertsRows shouldBe empty

    summaryRows should contain allElementsOf Seq(
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-1","level":"ERROR","total":1,"rate":0.2}"""),
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-2","level":"ERROR","total":2,"rate":0.4}"""),
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-3","level":"ERROR","total":1,"rate":0.2}""")
    )
  }

  it should "be able to compute average rate and total number of log messages per level" in withSpark { implicit spark =>
    import spark.implicits._

    // Prepare
    val memoryStream = new MemoryStream[String](1, spark.sqlContext)
    memoryStream.addData(
      """
        [
          {"timestamp":"2018-04-02T07:30:15.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:16.100Z","host":"host-1","level":"INFO","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:17.100Z","host":"host-1","level":"INFO","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:18.100Z","host":"host-1","level":"TRACE","text":"Error description"}
        ]"""
    )
    val events = memoryStream.toDS()

    // Business methods
    val analyzer = new SparkLogAnalyzer(testWindowDuration, testSlideDuration)
    val (summary, alerts) = analyzer.setupProcessing(events)
    val (summaryRows, alertsRows) = processData(summary, alerts)

    // Asserts
    summaryRows should have length 3
    alertsRows shouldBe empty

    summaryRows should contain allElementsOf Seq(
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-1","level":"ERROR","total":1,"rate":0.2}"""),
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-1","level":"INFO","total":2,"rate":0.4}"""),
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-1","level":"TRACE","total":1,"rate":0.2}""")
    )
  }

  it should "be able to compute alerts if rate of ERRORs for host is greater than 1 per second" in withSpark { implicit spark =>
    import spark.implicits._

    // Prepare
    val memoryStream = new MemoryStream[String](1, spark.sqlContext)
    memoryStream.addData(
      """[
          {"timestamp":"2018-04-02T07:30:15.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:16.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:17.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:18.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:19.100Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:19.200Z","host":"host-1","level":"ERROR","text":"Error description"},
          {"timestamp":"2018-04-02T07:30:19.300Z","host":"host-1","level":"ERROR","text":"Error description"}
        ]"""
    )

    val events = memoryStream.toDS()

    // Business methods
    val analyzer = new SparkLogAnalyzer(testWindowDuration, testSlideDuration)
    val (summary, alerts) = analyzer.setupProcessing(events)
    val (summaryRows, alertsRows) = processData(summary, alerts)

    // Asserts
    summaryRows should have length 1
    alertsRows should have length 1

    summaryRows should contain only {
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-1","level":"ERROR","total":7,"rate":1.4}""")
    }

    alertsRows should contain only {
      RowFactory.create("""{"timestamp":"2018-04-02T07:30:20.000Z","host":"host-1","error_rate":1.4}""")
    }
  }

  private def processData(summary: DataStreamWriter[String], alerts: DataStreamWriter[String])(implicit spark: SparkSession) = {
    summary
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("summary")
      .start()
      .processAllAvailable()
    alerts
      .outputMode(OutputMode.Complete())
      .format("memory")
      .queryName("alerts")
      .start()
      .processAllAvailable()

    val summaryRows = spark.sql("select * from summary").collectAsList()
    val alertsRows = spark.sql("select * from alerts").collectAsList()

    (summaryRows, alertsRows)
  }
}
