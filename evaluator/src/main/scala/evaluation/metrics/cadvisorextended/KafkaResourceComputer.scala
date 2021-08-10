package evaluation.metrics.cadvisorextended

import evaluation.config.EvaluationConfig
import evaluation.utils.RunConfiguration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ceil, col, concat_ws, explode, expr, lag, lit, round, to_timestamp, typedLit}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class KafkaResourceComputer(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {

  import sparkSession.implicits._

  val runConfigColumns: Array[Column] = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)

  def wrangleKafkaCadvisorMetrics() {
    val cadvisorMetrics = sparkSession.read.option("inferSchema", true).json(evaluationConfig.cadvisorKafkaMetricsFrameworkPath)
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
        col("diskio.io_service_bytes.stats.Total").as("diskio_bytes_total"),
        col("diskio.io_service_bytes.stats.Read").as("diskio_bytes_read"),
        col("diskio.io_service_bytes.stats.Write").as("diskio_bytes_write"),
        col("memory.usage").as("memory_usage"),
        col("memory.cache").as("memory_cache"),
        col("memory.swap").as("memory_swap"),
        col("memory.rss").as("memory_rss"),
        col("memory.working_set").as("memory_working_set"),
        col("memory.mapped_file").as("memory_mapped"),
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
      .withColumn("cpu_total", col("cpu_total") / 1000000000)
      // rounding the columns with cores
      .withColumn("cpu_total", round(col("cpu_total"), 3))

      // sum up over filesystems, networks and disks
      .withColumn("diskio_bytes_total", expr("AGGREGATE(diskio_bytes_total, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("diskio_bytes_read", expr("AGGREGATE(diskio_bytes_read, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("diskio_bytes_write", expr("AGGREGATE(diskio_bytes_write, BIGINT(0), (accumulator, item) -> accumulator + item)"))

      // compute derivatives for accumulating columns
      .withColumn("time_diff", (col("time") - lag(col("time"), 1, null).over(Window.partitionBy("containerName").orderBy("time"))).divide(1000.0))

      .withColumn("network_rx_bytes", expr("AGGREGATE(network_rx_bytes, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("network_tx_bytes", expr("AGGREGATE(network_tx_bytes, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("network_rx_dropped", expr("AGGREGATE(network_rx_dropped, BIGINT(0), (accumulator, item) -> accumulator + item)"))
      .withColumn("network_tx_dropped", expr("AGGREGATE(network_tx_dropped, BIGINT(0), (accumulator, item) -> accumulator + item)"))
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
      .withColumn("diskio_MB_total", bytesToMB("diskio_bytes_total"))
      .withColumn("diskio_MB_write", bytesToMB("diskio_bytes_write"))
      .withColumn("diskio_MB_read", bytesToMB("diskio_bytes_read"))
      .withColumn("network_rxMbitps", bytesToMbits("network_rx_bytes"))
      .withColumn("network_txMbitps", bytesToMbits("network_tx_bytes"))

    cleanedCadvisorMetrics.show(20, false)
    cleanedCadvisorMetrics.printSchema()
    cleanedCadvisorMetrics
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "containerName", "runConfiguration.*", "time",
        // CPU
        "cpu_total",
        // DISK and FS
        "diskio_MB_total", "diskio_MB_read", "diskio_MB_write",
        "diskio_bytes_per_second", "diskio_bytes_read_per_second", "diskio_bytes_write_per_second",
        // MEMORY
        "memory_usage_MB", "memory_cache_MB", "memory_swap_MB", "memory_rss_MB", "memory_working_set_MB", "memory_mapped_MB",
        // NETWORK
        "network_rxMbitps", "network_txMbitps", "network_rx_dropped", "network_tx_dropped"
      )
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("kafka-cadvisor-metrics-per-container-timeseries"))
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