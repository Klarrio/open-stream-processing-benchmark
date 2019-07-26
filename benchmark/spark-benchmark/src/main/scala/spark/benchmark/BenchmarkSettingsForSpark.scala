package spark.benchmark

import com.typesafe.config.{Config, ConfigFactory}
import common.config.GeneralConfig
import common.config.JobExecutionMode.{CONSTANT_RATE, LATENCY_CONSTANT_RATE}

import scala.collection.JavaConverters._

object BenchmarkSettingsForSpark {

  implicit class OptionValue(val value: Option[String]) extends AnyVal {
    def keyedWith(key: String): Option[(String, String)] = value.map(v => key -> v)
  }

}

class BenchmarkSettingsForSpark(overrides: Map[String, String] = Map()) extends Serializable {

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
    else if (general.mode.equals(CONSTANT_RATE)) sparkProperties.getInt("streaming.batchInterval")
    else general.windowSlideIntervalMillis
    val parallelism: Int = sparkProperties.getInt("default.parallelism")
    val blockInterval: Int = math.max(batchInterval/parallelism, 50)
    val sqlShufflePartitions: Int = sparkProperties.getInt("sql.shuffle.partitions")
    val sqlMinBatchesToRetain: Int = sparkProperties.getInt("sql.streaming.minBatchesToRetain")
    val backpressureEnabled: Boolean = sparkProperties.getBoolean("streaming.backpressure.enabled")
    val localityWait: Int = sparkProperties.getInt("locality.wait")
    val jobProfileKey: String = general.mkJobProfileKey("spark", batchInterval)
  }

}
