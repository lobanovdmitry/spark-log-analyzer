
lazy val commonSettings = Seq(
  name := "spark-log-analyzer",
  version := "0.1.0",
  scalaVersion := "2.11.8"
)

lazy val assemblySettings = Seq(
  assemblyJarName := "spark-log-analyzer.jar",
  assemblyMergeStrategy in assembly := {
    case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
    case x => (assemblyMergeStrategy in assembly).value(x)
  },
  test in assembly := {}
)

lazy val sparkVersion = "2.3.0"

lazy val app = (project in file("."))
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion exclude("net.jpountz.lz4", "lz4"),
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion exclude("net.jpountz.lz4", "lz4"),
      "net.liftweb" %% "lift-json" % "3.2.0",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  )
