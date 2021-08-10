package evaluation.metrics.jmx

import evaluation.config.EvaluationConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class TaskInfo(taskName: String, containerId: String, startTime: Long, endTime: Long)

class JmxMemoryUtils(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)

  def compute(): DataFrame={
    val memoryMetrics: DataFrame = getData()
    memoryMetrics.persist()
    val runConfigColumns = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
    memoryMetrics
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .withColumnRenamed("cpuLoad", "systemCpuUsage")
      .withColumnRenamed("sunProcessCpuLoad", "processCpuUsage")
      .withColumnRenamed("javaLangCpu", "systemLoadAverage")
      .select("runConfigKey", "containerName", "runConfiguration.*", "time", "heapUsageMB", "nonHeapUsageMB", "systemCpuUsage", "processCpuUsage", "systemLoadAverage")
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("resources-per-container-timeseries"))

    val workerMemoryMetrics  = memoryMetrics.filter(!col("containerName").contains("jobmanager") && !col("containerName").contains("master"))
    workerMemoryMetrics.persist()
    val resourceMetricsStats = computeMetrics(workerMemoryMetrics.toDF)
    resourceMetricsStats
  }

  def getData(): DataFrame ={
    val jmxMetrics = sparkSession.read.json(evaluationConfig.resourceMetricsFrameworkPath)

    val resourceMetrics = jmxMetrics.join(runTimes, jmxMetrics("time") < runTimes("endTime") && jmxMetrics("time") > runTimes("beginTime"))
      .drop("beginTime", "endTime")
      .withColumn("nonHeapUsed", col("nonHeapUsed") / (1024 * 1024))
      .withColumn("nonHeapCommitted", col("nonHeapCommitted") / (1024 * 1024))
      .withColumn("heapUsed", col("heapUsed") / (1024 * 1024))
      .withColumn("heapCommitted", col("heapCommitted") / (1024 * 1024))

      .withColumn("nonHeapUsageMB", col("nonHeapUsed"))
      .withColumn("heapUsageMB", col("heapUsed"))
      .withColumn("heapCommittedMB", col("heapUsed"))

    resourceMetrics
  }

  def computeMetrics(data: DataFrame): DataFrame = {
    data.createOrReplaceTempView("resourceMetricsTable")
    val resourceMetrics = sparkSession.sql(
      """select runConfiguration,
        |avg(heapUsageMB) as mean_mem,
        |stddev(heapUsageMB) as stddev_mem,
        |min(heapUsageMB) as  min_mem,
        |max(heapUsageMB) as max_mem,
        |percentile(heapUsageMB,0.01) as p1_mem,
        |percentile(heapUsageMB,0.05) as p5_mem,
        |percentile(heapUsageMB,0.25) as p25_mem,
        |percentile(heapUsageMB,0.5) as p50_mem,
        |percentile(heapUsageMB,0.75) as p75_mem,
        |percentile(heapUsageMB,0.95) as p95_mem,
        |percentile(heapUsageMB,0.99) as p99_mem,
        |percentile(heapUsageMB,0.999) as p999_mem,
        |percentile(heapUsageMB,0.99999) as p99999_mem,
        |avg(nonHeapUsageMB) as mean_non_heap,
        |stddev(nonHeapUsageMB) as stddev_non_heap,
        |min(nonHeapUsageMB) as  min_non_heap,
        |max(nonHeapUsageMB) as max_non_heap,
        |percentile(nonHeapUsageMB,0.01) as p1_non_heap,
        |percentile(nonHeapUsageMB,0.05) as p5_non_heap,
        |percentile(nonHeapUsageMB,0.25) as p25_non_heap,
        |percentile(nonHeapUsageMB,0.5) as p50_non_heap,
        |percentile(nonHeapUsageMB,0.75) as p75_non_heap,
        |percentile(nonHeapUsageMB,0.95) as p95_non_heap,
        |percentile(nonHeapUsageMB,0.99) as p99_non_heap,
        |percentile(nonHeapUsageMB,0.999) as p999_non_heap,
        |percentile(nonHeapUsageMB,0.99999) as p99999_non_heap
        |from resourceMetricsTable
        |group by runConfiguration
        |order by runConfiguration""".stripMargin
    )
    resourceMetrics
  }
}

