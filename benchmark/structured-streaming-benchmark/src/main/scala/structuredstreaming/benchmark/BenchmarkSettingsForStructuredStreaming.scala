package structuredstreaming.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import common.config.GeneralConfig

import scala.collection.JavaConverters._

object BenchmarkSettingsForStructuredStreaming {

  implicit class OptionValue(val value: Option[String]) extends AnyVal {
    def keyedWith(key: String): Option[(String, String)] = value.map(v => key -> v)
  }

}

class BenchmarkSettingsForStructuredStreaming(overrides: Map[String, String] = Map()) extends Serializable {

  val general = new GeneralConfig(overrides)

  object specific extends Serializable{
    private val sparkProperties: Config = ConfigFactory.load()
      .withFallback(ConfigFactory.parseMap(overrides.asJava))
      .withFallback(ConfigFactory.load("structuredstreaming.conf"))
      .getConfig("structuredstreaming")
      .getConfig(general.mode.name)

    val checkpointDir: String = if (general.local) general.configProperties.getString("spark.checkpoint.dir")
    else "hdfs://" + general.hdfsActiveNameNode + "/checkpointDirStructured" + System.currentTimeMillis() + "/"

    val sparkMaster: String = general.configProperties.getString("spark.master")
    val parallelism: Int = sparkProperties.getInt("default.parallelism")
    val sqlShufflePartitions: Int = sparkProperties.getInt("sql.shuffle.partitions")
    val blockInterval: Int = sparkProperties.getInt("block.interval.millis")
    val sqlMinBatchesToRetain: Int = sparkProperties.getInt("sql.streaming.minBatchesToRetain")
    val backpressureEnabled: Boolean = sparkProperties.getBoolean("streaming.backpressure.enabled")
    val localityWait: Int = sparkProperties.getInt("locality.wait")
    val watermarkMillis: Long = sparkProperties.getLong("watermark.ms")
    val useCustomTumblingWindow: Boolean = sparkProperties.getString("tumbling.window") == "custom"

    val jobProfileKey: String = general.mkJobProfileKey("structuredstreaming", general.windowSlideIntervalMillis)
  }
}
