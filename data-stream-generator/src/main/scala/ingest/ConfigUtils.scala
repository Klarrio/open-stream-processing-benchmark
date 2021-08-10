package ingest

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

object ConfigUtils extends Serializable {
  val configProperties: Config = ConfigFactory.load("resources.conf")
  configProperties.resolve()

  val generalConfigProps: Config = configProperties.getConfig("general")
  generalConfigProps.resolve()

  // General settings
  val local: Boolean = generalConfigProps.getString("run.local").equals("true")
  val mode: String = generalConfigProps.getString("mode")
  val lastStage: Int = generalConfigProps.getInt("last.stage").toInt
  val localPath: String = generalConfigProps.getString("local.path")
  val dataVolume: Int = generalConfigProps.getInt("data.volume")
  val publisherNb: String = generalConfigProps.getString("publisher.nb")

  // Kafka settings
  val kafkaBootstrapServers: String = configProperties.getString("kafka.bootstrap.servers")
  val flowTopic: String = configProperties.getString("kafka.flow.topic")
  val speedTopic: String = configProperties.getString("kafka.speed.topic")

  // AWS settings
  val s3Path: String = configProperties.getString("aws.s3.path") +"/time*.txt/part-00000-*.txt"
  val s3AccessKey: String = configProperties.getString("aws.s3.access.key")
  val s3SecretKey: String = configProperties.getString("aws.s3.secret.key")
}
