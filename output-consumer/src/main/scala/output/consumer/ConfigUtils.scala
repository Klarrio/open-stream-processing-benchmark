package output.consumer

import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

/**
 * Configuration of the output consumer
 * Comes from two sources:
 * - from the settings within the Spark context
 * - from the src/main/resources.conf file
 */
class ConfigUtils {
  val configProperties: Config = ConfigFactory.load("resources.conf")
  val systemTime: String = new Timestamp(System.currentTimeMillis()).toString.replaceAll(" ", "_").replaceAll(":", "_").replaceAll("\\.", "_")

  val local: Boolean = configProperties.getString("mode").contains("local")
}

class LocalConfigUtils extends ConfigUtils {
  val kafkaBootstrapServers: String = configProperties.getString("local.kafka.bootstrap.servers")
  val graphiteEnabled: Boolean = configProperties.getBoolean("local.graphite.enabled")
  val graphiteHost: String = configProperties.getString("local.graphite.host")
  val graphitePort: Int = configProperties.getInt("local.graphite.port")
  val stage: Int = configProperties.getInt("local.stage")
}

class ClusterConfigUtils(extraKeys: Map[String, String]) extends ConfigUtils {
  val framework: String = extraKeys("spark.FRAMEWORK")
  val mode: String = extraKeys("spark.MODE")
  val JOBUUID: String = extraKeys("spark.JOBUUID")
  val kafkaBootstrapServers: String = extraKeys("spark.KAFKA_BOOTSTRAP_SERVERS")

  val awsEndpoint: String = configProperties.getString("aws.endpoint")
  val awsAccessKey: String = extraKeys("spark.AWS_ACCESS_KEY")
  val awsSecretKey: String = extraKeys("spark.AWS_SECRET_KEY")

  val pathPrefix: String = extraKeys("spark.OUTPUT_METRICS_PATH")
  val path: String = pathPrefix + "/"  + framework + "/" + mode + "/observations-log-" + JOBUUID
  val metricsPath: String = pathPrefix + "/"  + framework + "/" + mode + "/metrics-log-" + JOBUUID
  val gcNotificationsPath: String = pathPrefix + "/"  + framework + "/" + mode + "/gc-log-" + JOBUUID
  val cadvisorPath: String = pathPrefix + "/" + framework + "/" + mode + "/cadvisor-log-" + JOBUUID
  val cadvisorHdfsPath: String = pathPrefix + "/"  + framework + "/" + mode + "/hdfs-cadvisor-log-" + JOBUUID
  val cadvisorKafkaPath: String = pathPrefix + "/"  + framework + "/" + mode + "/kafka-cadvisor-log-" + JOBUUID
}