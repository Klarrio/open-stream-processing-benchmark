package evaluation.metrics.cadvisorextended

import com.amazonaws.SDKGlobalConfiguration
import evaluation.EvaluationMain.initSpark
import evaluation.config.EvaluationConfig
import evaluation.utils.{IOUtils, RunConfiguration}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class CadvisorResourceComputer(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig){

  import sparkSession.implicits._

  val runConfigColumns: Array[Column] = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)

  /**
    * ROWS TO SELECT
    * cpu.usage.system
    * cpu.usage.total
    * cpu.usage.user
    *
    * diskio.io_wait
    *
    * filesystem[*].usage
    * filesystem[*].weighted_io_time
    * filesystem[*].io_time
    * filesystem[*].read_time
    * filesystem[*].write_time
    *
    * memory.usage
    * memory.cache
    * memory.swap
    * memory.container_data.pgfault
    * memory.container_data.pgmajfault
    *
    * network.interfaces[*].rx_bytes
    * network.interfaces[*].tx_bytes
    * network.interfaces[*].rx_dropped
    * network.interfaces[*].tx_dropped
    *
    * timestamp
    *
    */
  def wrangleCadvisorMetrics() {
    val cadvisorMetrics = sparkSession.read.option("inferSchema", true).json(evaluationConfig.cadvisorMetricsFrameworkPath)
      .select(col("containerName"), explode(col("value")).as("value"))
      .select("containerName", "value.*")
    cadvisorMetrics.printSchema()

    val parsedCadvisorMetrics = cadvisorMetrics
      .distinct()
      .select(
        col("containerName"),
        to_timestamp(col("timestamp")).as("timestamp"),
        col("cpu_inst.usage.system").as("cpu_system"),
        col("cpu_inst.usage.total").as("cpu_total"),
        col("cpu_inst.usage.user").as("cpu_user"),
        //    "diskio.io_wait",
        col("filesystem.usage").as("fs_usage"),
        col("filesystem.weighted_io_time").as("fs_weighted_io_time"),
        col("filesystem.io_time").as("fs_io_time"),
        col("filesystem.read_time").as("fs_read_time"),
        col("filesystem.write_time").as("fs_write_time"),
        col("diskio.io_service_bytes.stats.Total").as("diskio_bytes_total"),
        col("diskio.io_service_bytes.stats.Read").as("diskio_bytes_read"),
        col("diskio.io_service_bytes.stats.Write").as("diskio_bytes_write"),
        col("memory.usage").as("memory_usage"),
        col("memory.cache").as("memory_cache"),
        col("memory.swap").as("memory_swap"),
        col("memory.rss").as("memory_rss"),
        col("memory.working_set").as("memory_working_set"),
        col("memory.mapped_file").as("memory_mapped"),
        col("memory.container_data.pgfault").as("memory_pgfault"),
        col("memory.container_data.pgmajfault").as("memory_pgmajfault"),
        col("network.interfaces.rx_bytes").as("network_rx_bytes"),
        col("network.interfaces.tx_bytes").as("network_tx_bytes"),
        col("network.interfaces.rx_dropped").as("network_rx_dropped"),
        col("network.interfaces.tx_dropped").as("network_tx_dropped")
      ).withColumn("time", col("timestamp").cast("double") * 1000)

    val runTime = runTimes.select("runConfiguration.*").as[RunConfiguration].collect()(0)
    val endTime = runTimes.select("endTime").as[Long].collect()(0)
    val cleanedCadvisorMetrics = parsedCadvisorMetrics
      .withColumn("runConfiguration", typedLit(runTime))
      .withColumn("endTime", lit(endTime))
      .filter(col("time") < col("endTime") && col("time") > col("runConfiguration.startTime"))
      // THE ORDER IS IMPORTANT IN THE WITH COLUMN STATEMENTS!
      // nanocores to cores
      .withColumn("cpu_others", (col("cpu_total") - col("cpu_system") - col("cpu_user")) / 1000000000)
      .withColumn("cpu_system", col("cpu_system") / 1000000000)
      .withColumn("cpu_total", col("cpu_total") / 1000000000)
      .withColumn("cpu_user", col("cpu_user") / 1000000000)
      // compute percentages before rounding!
      .withColumn("cpu_others_pct", round((col("cpu_others")*100)/evaluationConfig.cpuPerWorker, 3))
      .withColumn("cpu_system_pct", round((col("cpu_system")*100)/evaluationConfig.cpuPerWorker, 3))
      .withColumn("cpu_total_pct", round((col("cpu_total")*100)/evaluationConfig.cpuPerWorker, 3))
      .withColumn("cpu_user_pct", round((col("cpu_user")*100)/evaluationConfig.cpuPerWorker, 3))
      // rounding the columns with cores
      .withColumn("cpu_system", round(col("cpu_system"), 3))
      .withColumn("cpu_total", round(col("cpu_total"), 3))
      .withColumn("cpu_user", round(col("cpu_user"), 3))

      // sum up over filesystems, networks and disks
      .withColumn("fs_usage", expr("AGGREGATE(fs_usage, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("fs_weighted_io_time", expr("AGGREGATE(fs_weighted_io_time, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("fs_io_time", expr("AGGREGATE(fs_io_time, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("fs_read_time", expr("AGGREGATE(fs_read_time, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("fs_write_time", expr("AGGREGATE(fs_write_time, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("network_rx_bytes", expr("AGGREGATE(network_rx_bytes, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("network_tx_bytes", expr("AGGREGATE(network_tx_bytes, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("network_rx_dropped", expr("AGGREGATE(network_rx_dropped, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("network_tx_dropped", expr("AGGREGATE(network_tx_dropped, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("diskio_bytes_total", expr("AGGREGATE(diskio_bytes_total, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("diskio_bytes_read", expr("AGGREGATE(diskio_bytes_read, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("diskio_bytes_write", expr("AGGREGATE(diskio_bytes_write, BIGINT(0), (accumulator, item) -> accumulator + item)"))

      // compute derivatives for accumulating columns
      .withColumn("time_diff", (col("time") - lag(col("time"), 1, null).over(Window.partitionBy("containerName").orderBy("time"))).divide(1000.0))

      .withColumn("memory_pgfault", computeDerivativeAndCeil("memory_pgfault"))
      .withColumn("memory_pgmajfault", computeDerivativeAndCeil("memory_pgmajfault"))
      .withColumn("fs_weighted_io_time", computeDerivative("fs_weighted_io_time"))
      .withColumn("fs_io_time", computeDerivative("fs_io_time"))
      .withColumn("fs_read_time", computeDerivative("fs_read_time"))
      .withColumn("fs_write_time", computeDerivative("fs_write_time"))
      .withColumn("network_rx_bytes", computeDerivativeAndCeil("network_rx_bytes"))
      .withColumn("network_tx_bytes", computeDerivativeAndCeil("network_tx_bytes"))
      .withColumn("network_rx_dropped", computeDerivativeAndCeil("network_rx_dropped"))
      .withColumn("network_tx_dropped", computeDerivativeAndCeil("network_tx_dropped"))
      .withColumn("diskio_bytes_per_second", computeDerivativeAndCeil("diskio_bytes_total"))
      .withColumn("diskio_bytes_read_per_second", computeDerivativeAndCeil("diskio_bytes_read"))
      .withColumn("diskio_bytes_write_per_second", computeDerivativeAndCeil("diskio_bytes_write"))

      // convert bytes to KB or MB
      .withColumn("memory_usage_MB", bytesToMB("memory_usage"))
      .withColumn("memory_cache_MB", bytesToMB("memory_cache"))
      .withColumn("memory_swap_MB", bytesToMB("memory_swap"))
      .withColumn("memory_rss_MB", bytesToMB("memory_rss"))
      .withColumn("memory_working_set_MB", bytesToMB("memory_working_set"))
      .withColumn("memory_mapped_MB", bytesToMB("memory_mapped"))
      .withColumn("fs_usage_MB", bytesToMB("fs_usage"))
      .withColumn("diskio_MB_total", bytesToMB("diskio_bytes_total"))
      .withColumn("diskio_MB_read", bytesToMB("diskio_bytes_read"))
      .withColumn("diskio_MB_write", bytesToMB("diskio_bytes_write"))

      .withColumn("network_rxMbitps", bytesToMbits("network_rx_bytes"))
      .withColumn("network_txMbitps", bytesToMbits("network_tx_bytes"))

      .drop("time_diff", "network_tx_bytes", "network_rx_bytes")

    cleanedCadvisorMetrics.show(20, false)
    cleanedCadvisorMetrics.printSchema()
    cleanedCadvisorMetrics
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "containerName", "runConfiguration.*", "time",
        // CPU
        "cpu_total", "cpu_system", "cpu_user", "cpu_others",
        "cpu_total_pct", "cpu_system_pct", "cpu_user_pct", "cpu_others_pct",
        // DISK and FS
        "diskio_MB_total", "diskio_MB_read", "diskio_MB_write",
        "diskio_bytes_per_second", "diskio_bytes_read_per_second", "diskio_bytes_write_per_second",
        "fs_usage_MB", "fs_weighted_io_time", "fs_io_time", "fs_read_time", "fs_write_time",
        // MEMORY
        "memory_usage_MB", "memory_cache_MB", "memory_swap_MB", "memory_rss_MB", "memory_working_set_MB", "memory_mapped_MB",
        "memory_pgfault", "memory_pgmajfault",
        // NETWORK
        "network_rxMbitps", "network_txMbitps", "network_rx_dropped", "network_tx_dropped"
      )
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("cadvisor-metrics-per-container-timeseries"))

  }


  def computeDerivativeAndCeil(columnName: String): Column = {
    ceil((col(columnName) - lag(columnName, 1, 0).over(Window.partitionBy("containerName").orderBy("time"))) /
      col("time_diff"))
  }

  def computeDerivative(columnName: String): Column = {
    ceil((col(columnName) - lag(columnName, 1, 0).over(Window.partitionBy("containerName").orderBy("time"))) /
      col("time_diff"))
  }

  def bytesToMB(columnName: String): Column = {
    round(col(columnName).divide(1024 * 1024), 2)
  }

  def bytesToMbits(columnName: String): Column = {
    round(col(columnName).multiply(8).divide(1024 * 1024), 2)
  }

}
