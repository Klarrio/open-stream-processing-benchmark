import sbt._
import Keys._

object Versions {
  val asyncHttpClient = "2.1.0-alpha22"

  val httpClient = "4.2"
  val hadoopAws = "2.7.3"

  val spark = "3.0.0"
  val scalaBinary = "2.12"

  val typeSafe = "1.3.1"
  val typeSafePlay = "2.9.0"

  val scalaInfluxDBconnector = "0.5.2"
}


object Dependencies {
  val evaluation: Seq[ModuleID] = Seq(
    "org.apache.spark" % s"spark-core_${Versions.scalaBinary}" % Versions.spark % "provided",
    "org.apache.spark" % s"spark-sql_${Versions.scalaBinary}" % Versions.spark % "provided",
    "org.apache.spark" % s"spark-mllib_${Versions.scalaBinary}" % Versions.spark % "provided"
  )


  val typeSafe: Seq[ModuleID] = Seq("com.typesafe.play" % s"play-json_${Versions.scalaBinary}" % Versions.typeSafePlay,
    "com.typesafe" % "config" % Versions.typeSafe,

    "org.asynchttpclient" % "async-http-client" % Versions.asyncHttpClient,
    "com.paulgoldbaum" %% "scala-influxdb-client" % Versions.scalaInfluxDBconnector exclude("org.asynchttpclient", "async-http-client"),

    "org.apache.httpcomponents" % "httpclient" % Versions.httpClient,

    //"org.apache.hadoop" % "hadoop-aws" % Versions.hadoopAws,
    "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "io.netty" % "netty-all" % "4.1.17.Final"

  ).map(_.exclude("com.fasterxml.jackson.core", "jackson-core")
    .exclude("com.fasterxml.jackson.core", "jackson-annotations")
    .exclude("com.fasterxml.jackson.core", "jackson-databind"))

  val jacksonDependencyOverrides = Set("com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7")
}