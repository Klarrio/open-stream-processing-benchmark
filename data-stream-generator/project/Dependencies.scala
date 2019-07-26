import sbt._

object Versions {
  val dropwizardMetrics = "3.2.2"
  val typeSafe = "1.3.1"
  val scalaBinary = "2.11"
  val kafka = "0.10.2.1"
  val logback = "1.2.2"
  val spark = "2.2.0"
}

object Dependencies {
  val sparkDependencies = Seq(
    "org.apache.spark" % s"spark-core_${Versions.scalaBinary}" % Versions.spark,
    "org.apache.spark" % s"spark-sql_${Versions.scalaBinary}" % Versions.spark
  ).map(_.exclude("org.slf4j", "slf4j-log4j12"))
  val rootDependencies: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % Versions.typeSafe,
    "io.dropwizard.metrics" % "metrics-core" % Versions.dropwizardMetrics,
    "org.apache.kafka" % s"kafka_${Versions.scalaBinary}" % Versions.kafka,
    "ch.qos.logback" % "logback-classic" % Versions.logback,
    "org.apache.hadoop" % "hadoop-aws" % "3.0.0-alpha2",
    "org.apache.hadoop" % "hadoop-hdfs" % "2.8.1"
  ).map(_.exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12").exclude("com.fasterxml.jackson.core", "jackson-core")
    .exclude("com.fasterxml.jackson.core", "jackson-annotations")
    .exclude("com.fasterxml.jackson.core", "jackson-databind")) ++ sparkDependencies
}