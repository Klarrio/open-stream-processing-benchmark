import sbt._

object Versions {
  val asyncHttpClient = "2.1.0-alpha22"
  val circe = "0.11.1"
  val httpClient = "4.2"
  val typeSafe = "1.3.1"
  val scalaBinary = "2.11"
  val kafka = "0.10.2.1"
  val logback = "1.2.2"
}

object Dependencies {
  val rootDependencies: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % Versions.typeSafe,
    "org.apache.kafka" % s"kafka_${Versions.scalaBinary}" % Versions.kafka,
    "ch.qos.logback" % "logback-classic" % Versions.logback,
    "io.circe" %% "circe-core" % Versions.circe,
    "io.circe" %% "circe-generic" % Versions.circe,
    "io.circe" %% "circe-parser" % Versions.circe,
    "org.asynchttpclient" % "async-http-client" % Versions.asyncHttpClient,
    "org.apache.httpcomponents" % "httpclient" % Versions.httpClient
  ).map(_.exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12"))
}