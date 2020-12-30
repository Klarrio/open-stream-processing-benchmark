
import sbt._
import Keys._

object Dependencies {

  object Versions {
    // The versions in alphabetic order
    val circe = "0.12.0-M3"

    val flink = "1.11.1"

    val kafka = "0.10.2.1"
    val kafkaBinary = "0.10"
    val kafkaStreams = "2.6.0"

    val logback = "1.2.2"

    val spark = "3.0.0"
    val structuredStreaming = "3.0.0"
    val sparkTestingBase = "2.4.5_0.14.0"
    val scalaBinary = "2.12"
    val scala = "2.12.8"
    val scalaTest = "3.0.5"

    val typeSafe = "1.3.1"
  }

  val commonDependencies: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % Versions.typeSafe,
    "ch.qos.logback" % "logback-classic" % Versions.logback,
    "io.circe" %% "circe-core" % Versions.circe,
    "io.circe" %% "circe-generic" % Versions.circe,
    "io.circe" %% "circe-parser" % Versions.circe,
    "org.scalatest" %% "scalatest" % Versions.scalaTest % Test
  ).map(_.exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12"))

  val flinkDependencies: Seq[ModuleID] = commonDependencies ++ Seq(
    "org.apache.flink" % "flink-core" % Versions.flink,
    "org.apache.flink" %% s"flink-streaming-scala" % Versions.flink,
    "org.apache.flink" %% s"flink-clients" % Versions.flink,
    "org.apache.flink" %% s"flink-connector-kafka" % Versions.flink,
    "org.apache.flink" %% "flink-statebackend-rocksdb" % Versions.flink,
    "org.apache.flink" %% s"flink-test-utils" % Versions.flink % Test,
    "org.apache.flink" %% s"flink-streaming-java" % Versions.flink % Test classifier "tests",
    "org.apache.flink" %% s"flink-runtime" % Versions.flink % Test classifier "tests"
  ).map(_.exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12"))

  val kafkaDependencies: Seq[ModuleID] = commonDependencies ++ Seq(
    "org.apache.kafka" % "kafka-clients" % Versions.kafkaStreams,
    "org.apache.kafka" % "kafka-streams" % Versions.kafkaStreams,
    "org.apache.kafka" %% "kafka-streams-scala" % Versions.kafkaStreams,

    "org.apache.kafka" % "kafka-streams-test-utils" % Versions.kafkaStreams % Test
  ).map(_.exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12"))

  val sparkDependencies: Seq[ModuleID] =
    commonDependencies ++ Seq(
      "org.apache.spark" %% "spark-core" % Versions.spark % Provided,
      "org.apache.spark" %% "spark-sql" % Versions.spark % Provided,
      "org.apache.spark" %% "spark-streaming" % Versions.spark % Provided,

      "org.apache.spark" %% "spark-streaming-kafka-0-10" % Versions.spark,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark,
      "com.holdenkarau" %% "spark-testing-base" % Versions.sparkTestingBase % Test
    ).map(_.exclude("org.slf4j", "slf4j-log4j12"))

  val structuredStreamingDependencies: Seq[ModuleID] =
    commonDependencies ++ Seq(
      "org.apache.spark" %% "spark-core" % Versions.structuredStreaming % Provided,
      "org.apache.spark" %% "spark-sql" % Versions.structuredStreaming % Provided,
      "org.apache.spark" %% "spark-streaming" % Versions.structuredStreaming % Provided,

      "org.apache.spark" %% "spark-streaming-kafka-0-10" % Versions.structuredStreaming,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.structuredStreaming,

      "com.holdenkarau" %% "spark-testing-base" % Versions.sparkTestingBase % Test
    ).map(_.exclude("org.slf4j", "slf4j-log4j12"))
}