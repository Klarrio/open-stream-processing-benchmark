package kafka.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import common.config.GeneralConfig

import scala.collection.JavaConverters._

object BenchmarkSettingsForKafkaStreams {

  implicit class OptionValue(val value: Option[String]) extends AnyVal {
    def keyedWith(key: String): Option[(String, String)] = value.map(v => key -> v)
  }
}

class BenchmarkSettingsForKafkaStreams(overrides: Map[String, String] = Map()) extends Serializable {

  val general = new GeneralConfig(overrides)

  object specific extends Serializable{
    private val kafkaStreamsProperties: Config = ConfigFactory.load()
      .withFallback(ConfigFactory.parseMap(overrides.asJava))
      .withFallback(ConfigFactory.load("kafkastreams.conf"))
      .getConfig("kafkastreams")
      .getConfig(general.mode.name)

    val applicationId: String = if (general.local) general.outputTopic + System.currentTimeMillis() else general.outputTopic

    val numStreamsThreads: Int = kafkaStreamsProperties.getInt("num.streams.threads")
    val commitInterval: Int =  kafkaStreamsProperties.getInt("commit.interval.ms")
    val cacheMaxBytesBuffering: Long = kafkaStreamsProperties.getLong("cache.max.bytes.buffering")
    val compressionTypeConfig: String = kafkaStreamsProperties.getString("compression.type.config")
    val batchSize: Long = kafkaStreamsProperties.getLong("batch.size")
    val lingerMs: Int = kafkaStreamsProperties.getInt("linger.ms")
    val gracePeriodMillis: Int = kafkaStreamsProperties.getInt("grace.period.ms")
    val maxTaskIdleMillis: String = kafkaStreamsProperties.getString("max.task.idle.ms")
    val kafkaCheckpointDir: String = general.configProperties.getString("kafkastreams.checkpoint.dir")
    val useCustomTumblingWindow: Boolean = kafkaStreamsProperties.getString("tumbling.window") == "custom"
    val useCustomSlidingWindow: Boolean = kafkaStreamsProperties.getString("sliding.window") == "custom"

    val jobProfileKey: String = general.mkJobProfileKey("kafka", lingerMs)

  }
}
