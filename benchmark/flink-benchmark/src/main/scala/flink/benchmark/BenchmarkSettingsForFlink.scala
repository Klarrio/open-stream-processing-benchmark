package flink.benchmark

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import common.config.GeneralConfig
import org.apache.flink.util.FileUtils

import scala.collection.JavaConverters._
import scala.util.Try

class BenchmarkSettingsForFlink(overrides: Map[String, String] = Map()) extends Serializable {
  val general = new GeneralConfig(overrides)

  object specific extends Serializable {
    private val flinkProperties: Config = ConfigFactory.load()
      .withFallback(ConfigFactory.parseMap(overrides.asJava))
      .withFallback(ConfigFactory.load("flink.conf"))
      .getConfig("flink")
      .getConfig(general.mode.name)

    val autoWatermarkInterval: Int = flinkProperties.getInt("auto.watermark.interval")
    val maxOutOfOrderness: Int = flinkProperties.getInt("max.out.of.orderness")
    val bufferTimeout: Long = flinkProperties.getLong("buffer.timeout")
    val checkpointInterval: Int = flinkProperties.getInt("checkpoint.interval")
    val useCustomTumblingWindowTrigger: Boolean = flinkProperties.getString("tumbling.window.trigger") == "custom"
    val useCustomSlidingWindowTrigger: Boolean = flinkProperties.getString("sliding.window.trigger") == "custom"

    // Checkpointing
    val checkpointDir: String = if (general.local) {
      val checkpointDir = new File(general.configProperties.getString("flink.checkpoint.dir"))
      Try(FileUtils.cleanDirectory(checkpointDir))
      "file://" + checkpointDir.getCanonicalPath
    } else general.configProperties.getString("flink.checkpoint.dir") + System.currentTimeMillis() + "/"


    val jobProfileKey: String = general.mkJobProfileKey("flink", bufferTimeout)
  }
}
