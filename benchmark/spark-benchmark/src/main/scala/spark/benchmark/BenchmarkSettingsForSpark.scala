package spark.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import common.config.GeneralConfig
import common.config.JobExecutionMode.{CONSTANT_RATE, LATENCY_CONSTANT_RATE}
import common.config.LastStage.{NON_INCREMENTAL_WINDOW_WITHOUT_JOIN, REDUCE_WINDOW_WITHOUT_JOIN}

import scala.collection.JavaConverters._

object BenchmarkSettingsForSpark {

  implicit class OptionValue(val value: Option[String]) extends AnyVal {
    def keyedWith(key: String): Option[(String, String)] = value.map(v => key -> v)
  }

}

class BenchmarkSettingsForSpark(overrides: Map[String, Any] = Map()) extends Serializable {

  val general = new GeneralConfig(overrides)

  object specific extends Serializable {
    private val sparkProperties: Config = ConfigFactory.load()
      .withFallback(ConfigFactory.parseMap(overrides.asJava))
      .withFallback(ConfigFactory.load("spark.conf"))
      .getConfig("spark")
      .getConfig(general.mode.name)

    val checkpointDir: String = if (general.local) general.configProperties.getString("spark.checkpoint.dir")
    else "hdfs://" + general.hdfsActiveNameNode + "/checkpointDirStructured" + System.currentTimeMillis() + "/"

    val sparkMaster: String = general.configProperties.getString("spark.master")

    val batchInterval: Int = if (general.lastStage.value < 2 & general.mode.equals(LATENCY_CONSTANT_RATE)) 200
    else if (general.lastStage == REDUCE_WINDOW_WITHOUT_JOIN || general.lastStage == NON_INCREMENTAL_WINDOW_WITHOUT_JOIN) general.slideDurationMsOfWindowAfterParse
    else 1000

    val defaultParallelism: Int = general.configProperties.getInt("spark.default.parallelism")
    val sqlShufflePartitions: Int = general.configProperties.getInt("spark.sql.shuffle.partitions")
    val blockInterval: Int = Math.min(batchInterval/defaultParallelism, 50)
    val localityWait: Int = sparkProperties.getInt("locality.wait")
    val sqlMinBatchesToRetain: Int = sparkProperties.getInt("sql.streaming.minBatchesToRetain")
    val writeAheadLogEnabled: Boolean = sparkProperties.getBoolean("spark.streaming.receiver.writeAheadLog.enable")
    val jobProfileKey: String = general.mkJobProfileKey("spark", batchInterval)
  }
}
