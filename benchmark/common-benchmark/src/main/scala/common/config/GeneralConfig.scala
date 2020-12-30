package common.config

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.util.Try

class GeneralConfig(overrides: Map[String, Any] = Map()) extends Serializable {
  // "aws", "local" or "docker"
  val deploymentType: String = Try(sys.env("DEPLOYMENT_TYPE")).getOrElse("aws")

  val configProperties: Config = ConfigFactory.load()
    .withFallback(ConfigFactory.parseMap(overrides.asJava))
    .withFallback(ConfigFactory.load("commonsettings.conf"))
    .withFallback(ConfigFactory.load(deploymentType + ".conf"))

//  print(configProperties)

  // Environment Configuration
  val mode: JobExecutionMode = JobExecutionMode.withName(configProperties.getString("environment.mode"))

  val local: Boolean = deploymentType == "local"


  // Mode dependent configurations
  val modeConfig: Config = configProperties.getConfig(mode.name)
  // Intervals at which new timestamps are published and at which windows slide
  val publishIntervalMillis: Int = modeConfig.getInt("stream.source.publish.interval.millis")

  //GENERAL
  val jobId: String = UUID.randomUUID().toString
  lazy val hdfsActiveNameNode: String = configProperties.getString("hdfs.active.name.node")

  // Kafka Configuration
  val kafkaConfig: Config = configProperties.getConfig("kafka")
  lazy val kafkaBootstrapServers: String = kafkaConfig.getString("bootstrap.servers")
  lazy val zookeeperServer: String = kafkaConfig.getString("zookeeper.server")
  val groupId: String = kafkaConfig.getString("groupid") + System.currentTimeMillis().toString
  // The Kafka reset setting. Latest in case of latency measurement and earliest in case of processing a single burst.
  val kafkaAutoOffsetReset: String = kafkaConfig.getString("auto.offset.reset.strategy")
  // Topic where the output is written to
  val outputTopic: String = kafkaConfig.getString("output.topic")
  val flowTopic: String = kafkaConfig.getString("flow.topic")
  val speedTopic: String = kafkaConfig.getString("speed.topic")

  // Monitoring Configurations
  // Graphite port for metric visualization in Grafana
  val graphiteHost: String = configProperties.getString("monitoring.graphite.host")
  val graphitePort: Int = configProperties.getInt("monitoring.graphite.port")
  val shouldPrintOutput: Boolean = configProperties.getBoolean("monitoring.print.output")

  //if you just want to execute part of the workflow, set the environment variable LAST_STAGE to one of these stages
  val lastStage: LastStage = LastStage.withName(configProperties.getString("general.last.stage").toInt)

  // Volume of data that is published to Kafka
  val volume: Int = configProperties.getString("general.stream.source.volume").toInt

  // Lookback periods for the final sliding window stage when executed after the aggregation stage.
  val shortTermBatchesLookback: Int = configProperties.getInt("general.sliding.window.after.aggregation.short.term.batches.lookback")
  val longTermBatchesLookback: Int = configProperties.getInt("general.sliding.window.after.aggregation.long.term.batches.lookback")
  val longWindowLengthMillis: Int = (longTermBatchesLookback + 1) * publishIntervalMillis

  // Windowing periods when the join is not executed.
  val windowDurationMsOfWindowAfterParse: Int = configProperties.getInt("general.window.after.parsing.window.duration")
  val slideDurationMsOfWindowAfterParse: Int = configProperties.getInt("general.window.after.parsing.slide.duration")

  //To get the key of the messages of the messages that the frameworks put on Kafka
  private val now: Long = System.currentTimeMillis()

  def mkJobProfileKey(framework: String, bufferTimeout: Long): String = {
    s"${framework}_i${jobId}_e${lastStage.value}_v${volume}_b${bufferTimeout}_s${shortTermBatchesLookback}_l${longTermBatchesLookback}_t${now}"
  }
}
