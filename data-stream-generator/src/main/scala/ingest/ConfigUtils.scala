package ingest

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

object ConfigUtils extends Serializable {
  val configProperties: Config = ConfigFactory.load("resources.conf")

  val generalConfigProps: Config = configProperties.getConfig("general")

  // General settings
  lazy val local: Boolean = generalConfigProps.getString("run.local").equals("true")
  lazy val mode: String = generalConfigProps.getString("mode")
  lazy val lastStage: Int = generalConfigProps.getInt("last.stage").toInt
  lazy val localPath: String = generalConfigProps.getString("local.path")
  lazy val dataVolume: Int = generalConfigProps.getInt("data.volume")
  lazy val publisherNb: String = generalConfigProps.getString("publisher.nb")

  // Kafka settings
  lazy val kafkaBootstrapServers: String = configProperties.getString("kafka.bootstrap.servers")
  lazy val flowTopic: String = configProperties.getString("kafka.flow.topic")
  lazy val speedTopic: String = configProperties.getString("kafka.speed.topic")

  // AWS settings
  lazy val s3Path: String = configProperties.getString("aws.s3.path")
  lazy val s3AccessKey: String = configProperties.getString("aws.s3.access.key")
  lazy val s3SecretKey: String = configProperties.getString("aws.s3.secret.key")
}
