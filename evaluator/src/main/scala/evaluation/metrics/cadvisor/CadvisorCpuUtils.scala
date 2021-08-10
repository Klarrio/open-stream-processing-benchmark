package evaluation.metrics.cadvisor

import java.net.URLEncoder
import java.text.{DateFormat, SimpleDateFormat}
import java.util.SimpleTimeZone

import evaluation.config.EvaluationConfig
import evaluation.metrics.jmx.TaskInfo
import evaluation.utils.{CpuUsage, QueryResponse, RunConfiguration}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * CPU
  * - total
  * - user
  * - system
  *
  * @param runTimes
  * @param sparkSession
  * @param evaluationConfig
  */
class CadvisorCpuUtils(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import sparkSession.implicits._

  val runConfigColumns: Array[Column] = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)

  def getDataForMetric(metricName: String, columnNamePrefix: String, listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)]): DataFrame = {
    val cpuData = listOfTaskInfo.map {
      case (taskInfo: TaskInfo, runConfiguration: RunConfiguration, condition: String) =>
        val queryString = s"SELECT derivative(value)/1000000000 from $metricName  WHERE value>0 AND container_name =~ /^*${taskInfo.containerId}/ $condition"
        logger.info(queryString)
        val encodedQuery = URLEncoder.encode(queryString, "UTF-8")
        val queryUrl = evaluationConfig.influxDbUrl + "/query?q=" + encodedQuery + "&db=cadvisor"
        val queryResponse = CadvisorQueryClient.getResponse(queryUrl)
        QueryResponse(taskInfo.containerId, taskInfo.taskName, runConfiguration, queryResponse)
    }.toList
    val cleanedCpuData = clean(cpuData)
    cleanedCpuData
      .withColumnRenamed("cpuUsage", columnNamePrefix + "Usage")
      .withColumnRenamed("cpuUsagePct", columnNamePrefix + "UsagePct")
  }

  /**
    * Parse Json of the results
    */
  private def clean(resourceUsageFiles: Seq[QueryResponse]): DataFrame = {
    val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
    val cpuUsageList = resourceUsageFiles.flatMap {
      queryResponse =>
        val values = Try {
          queryResponse.queryResponse("results").asInstanceOf[List[Map[String, Any]]]
            .head("series").asInstanceOf[List[Map[String, Any]]]
            .head("values").asInstanceOf[List[List[Any]]]
        }.toOption
        values.getOrElse(List()).map { element =>
          val timestamp = dateFormat.parse(element.head.toString).getTime
          val cpuUsage = element(1).toString.toFloat
          val cpuUsagePct = 100.0 * (cpuUsage / evaluationConfig.cpuPerWorker)
          CpuUsage(queryResponse.taskName, queryResponse.containerId, queryResponse.runConfig, timestamp, cpuUsage, cpuUsagePct)
        }
    }
    sparkSession.sparkContext.parallelize(cpuUsageList)
      .toDF()
  }

  def writeCleanedCpuData(cleanedCpuData: DataFrame): Unit = {
    cleanedCpuData
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "containerName", "containerTaskId", "runConfiguration.*", "time",
        "cpuUsage", "cpuUsagePct",
        "userCpuUsage", "userCpuUsagePct",
        "systemCpuUsage", "systemCpuUsagePct"
      )
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("cpu-per-container-timeseries"))
  }


  def computeMetrics(data: DataFrame): DataFrame = {
    data
      .filter(!col("containerName").contains("jobmanager") && !col("containerName").contains("master"))
      .toDF
      .createOrReplaceTempView("cpuTable")
    val cpuMetrics = sparkSession.sql(
      """select runConfiguration,
        				|avg(cpuUsage) as mean_cpu,
        				|stddev(cpuUsage) as stddev_cpu,
        				|min(cpuUsage) as  min_cpu,
        				|max(cpuUsage) as max_cpu,
        |percentile(cpuUsage,0.01) as p01_cpu,
        				|percentile(cpuUsage,0.05) as p5_cpu,
        				|percentile(cpuUsage,0.25) as p25_cpu,
        				|percentile(cpuUsage,0.5) as p50_cpu,
        				|percentile(cpuUsage,0.75) as p75_cpu,
        				|percentile(cpuUsage,0.95) as p95_cpu,
        				|percentile(cpuUsage,0.99) as p99_cpu,
        |percentile(cpuUsage,0.999) as p999_cpu,
        |percentile(cpuUsage,0.99999) as p99999_cpu
        				|from cpuTable
        				|group by runConfiguration
        				|order by runConfiguration""".stripMargin
    )
    cpuMetrics
  }
}
