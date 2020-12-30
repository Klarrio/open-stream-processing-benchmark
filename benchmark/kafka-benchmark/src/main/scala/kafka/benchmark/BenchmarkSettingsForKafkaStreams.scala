package kafka.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import common.config.GeneralConfig
import common.config.JobExecutionMode.{CONSTANT_RATE, LATENCY_CONSTANT_RATE}
import common.config.LastStage.{NON_INCREMENTAL_WINDOW_WITHOUT_JOIN, REDUCE_WINDOW_WITHOUT_JOIN}

import scala.collection.JavaConverters._

object BenchmarkSettingsForKafkaStreams {

  implicit class OptionValue(val value: Option[String]) extends AnyVal {
    def keyedWith(key: String): Option[(String, String)] = value.map(v => key -> v)
  }

}

class BenchmarkSettingsForKafkaStreams(overrides: Map[String, Any] = Map()) extends Serializable {

  val general = new GeneralConfig(overrides)

  object specific extends Serializable {
    private val kafkaStreamsProperties: Config = ConfigFactory.load()
      .withFallback(ConfigFactory.parseMap(overrides.asJava))
      .withFallback(ConfigFactory.load("kafkastreams.conf"))
      .getConfig("kafkastreams")
      .getConfig(general.mode.name)

    val applicationId: String = if (general.local) general.outputTopic + System.currentTimeMillis() else general.outputTopic

    val numStreamsThreadsPerInstance: Int = general.configProperties.getString("kafkastreams.streams.threads.per.instance").toInt
    val memory: Long = kafkaStreamsProperties.getLong("memory")

    val batchSize: Long = kafkaStreamsProperties.getLong("batch.size")
    val bufferMemorySize: Long = kafkaStreamsProperties.getLong("buffer.memory.bytes")
    val commitInterval: Int = if (general.lastStage ==  REDUCE_WINDOW_WITHOUT_JOIN || general.lastStage == NON_INCREMENTAL_WINDOW_WITHOUT_JOIN) general.slideDurationMsOfWindowAfterParse
    else 1000

    val cacheMaxBytesBuffering: Long = kafkaStreamsProperties.getLong("cache.max.bytes.buffering")
    val compressionTypeConfig: String = kafkaStreamsProperties.getString("compression.type.config")
    val fetchMinBytes: Long = kafkaStreamsProperties.getLong("fetch.min.bytes")
    val exactlyOnce: Boolean = kafkaStreamsProperties.getBoolean("exactly.once")
    val gracePeriodMillis: Int = kafkaStreamsProperties.getInt("grace.period.ms")
    val lingerMs: Int = kafkaStreamsProperties.getInt("linger.ms")
    val maxTaskIdleMillis: String = kafkaStreamsProperties.getString("max.task.idle.ms")

    val kafkaCheckpointDir: String = general.configProperties.getString("kafkastreams.checkpoint.dir")

    // intermediate topics and topics for state stores
    val speedThroughTopic: String = "speed-through-topic-" + general.outputTopic
    val flowThroughTopic: String = "flow-through-topic-" + general.outputTopic
    val aggregationThroughTopic: String = "aggregation-data-topic-" + general.outputTopic
    val nonIncrementalWindowThroughTopic: String = "non-incremental-window-data-topic-" + general.outputTopic
    val aggregatorStateStore: String = "lane-aggregator-state-store-" + general.outputTopic
    val relativeChangeStateStore: String = "relative-change-state-store-" + general.outputTopic
    val reduceWindowAfterParsingStateStore: String = "reduce-window-after-parsing-store-" + general.outputTopic
    val nonIncrementalWindowAfterParsingStateStore: String = "non-incremental-window-state-store-" + general.outputTopic

    val jobProfileKey: String = general.mkJobProfileKey("kafka", lingerMs)
  }
}
