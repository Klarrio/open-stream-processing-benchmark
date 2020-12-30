package structuredstreaming.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import common.config.GeneralConfig
import common.config.JobExecutionMode.{CONSTANT_RATE, LATENCY_CONSTANT_RATE}
import common.config.LastStage.{NON_INCREMENTAL_WINDOW_WITHOUT_JOIN, REDUCE_WINDOW_WITHOUT_JOIN}
import org.apache.spark.sql.streaming.Trigger

import scala.collection.JavaConverters._

object BenchmarkSettingsForStructuredStreaming {

  implicit class OptionValue(val value: Option[String]) extends AnyVal {
    def keyedWith(key: String): Option[(String, String)] = value.map(v => key -> v)
  }

}

class BenchmarkSettingsForStructuredStreaming(overrides: Map[String, Any] = Map()) extends Serializable {

  val general = new GeneralConfig(overrides)

  object specific extends Serializable{
    private val sparkProperties: Config = ConfigFactory.load()
      .withFallback(ConfigFactory.parseMap(overrides.asJava))
      .withFallback(ConfigFactory.load("structuredstreaming.conf"))
      .getConfig("structuredstreaming")
      .getConfig(general.mode.name)

    val checkpointDir: String = if (general.local) general.configProperties.getString("spark.checkpoint.dir")
    else "hdfs://" + general.hdfsActiveNameNode + "/checkpointDirStructured" + System.currentTimeMillis() + "/"

    val defaultParallelism: Int = general.configProperties.getInt("spark.default.parallelism")
    val sqlShufflePartitions: Int = general.configProperties.getInt("spark.sql.shuffle.partitions")
    val blockInterval: Int = 1000/defaultParallelism
    val sparkMaster: String = general.configProperties.getString("spark.master")
    val watermarkMillis: Long = sparkProperties.getLong("watermark.ms")
    val localityWait: Int = sparkProperties.getInt("locality.wait")
    val writeAheadLogEnabled: Boolean = sparkProperties.getBoolean("spark.streaming.receiver.writeAheadLog.enable")

    val trigger: Trigger = if(general.lastStage == REDUCE_WINDOW_WITHOUT_JOIN || general.lastStage == NON_INCREMENTAL_WINDOW_WITHOUT_JOIN) Trigger.ProcessingTime(general.slideDurationMsOfWindowAfterParse)
    else Trigger.ProcessingTime(0) // processing as fast as possible

    val jobProfileKey: String = general.mkJobProfileKey("structuredstreaming", general.publishIntervalMillis)
  }
}
