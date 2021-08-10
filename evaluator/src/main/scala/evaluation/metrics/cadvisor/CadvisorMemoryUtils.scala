package evaluation.metrics.cadvisor

import java.net.URLEncoder
import java.text.{DateFormat, SimpleDateFormat}
import java.util.SimpleTimeZone

import evaluation.config.EvaluationConfig
import evaluation.metrics.jmx.TaskInfo
import evaluation.utils.{MemUsage, PgFaults, QueryResponse, RunConfiguration}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * Helps with wrangling cadvisor memory data
  *
  * Memory stats that we get from cadvisor:
  * - Usage (bytes)
  * - Cache (bytes)
  * - Swap (bytes)
  * - Pgfault (count)
  * - Pgmajfault (count)
  *
  * @param runTimes
  * @param sparkSession
  * @param evaluationConfig
  */
class CadvisorMemoryUtils(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import sparkSession.implicits._

  val runConfigColumns: Array[Column] = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
  val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))

  def getDataForBytesMetric(metricName: String, columnName: String, listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)]): DataFrame = {
    val memoryData = listOfTaskInfo.map {
      case (taskInfo: TaskInfo, runConfiguration: RunConfiguration, condition: String) =>
        val queryString = s"SELECT value from $metricName  WHERE value>0 AND container_name =~ /^*${taskInfo.containerId}/ $condition"
        logger.info(queryString)
        val encodedQuery = URLEncoder.encode(queryString, "UTF-8")
        val queryUrl = evaluationConfig.influxDbUrl + "/query?q=" + encodedQuery + "&db=cadvisor"
        val queryResponse = CadvisorQueryClient.getResponse(queryUrl)
        QueryResponse(taskInfo.containerId, taskInfo.taskName, runConfiguration, queryResponse)
    }.toList
    val cleanedMemoryData = cleanBytesData(memoryData)
    cleanedMemoryData
      .withColumnRenamed("memUsageMB", columnName)
  }

  def getDataForPageFaultMetric(metricName: String, columnName: String, listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)]): DataFrame = {
    val pgfaultData = listOfTaskInfo.map {
      case (taskInfo: TaskInfo, runConfiguration: RunConfiguration, condition: String) =>
        val queryString = s"SELECT derivative(value) from $metricName  WHERE value>0 AND container_name =~ /^*${taskInfo.containerId}/ $condition"
        logger.info(queryString)
        val encodedQuery = URLEncoder.encode(queryString, "UTF-8")
        val queryUrl = evaluationConfig.influxDbUrl + "/query?q=" + encodedQuery + "&db=cadvisor"
        val queryResponse = CadvisorQueryClient.getResponse(queryUrl)
        QueryResponse(taskInfo.containerId, taskInfo.taskName, runConfiguration, queryResponse)
    }.toList
    val cleanedPgfaultData = cleanPgFaultData(pgfaultData)
    cleanedPgfaultData
      .withColumnRenamed("pgFaults", columnName)
  }


  def cleanBytesData(bytesData: Seq[QueryResponse]): Dataset[MemUsage] = {
    val memoryUsageList = bytesData.flatMap {
      queryResponse =>
        val values = Try {
          queryResponse.queryResponse("results").asInstanceOf[List[Map[String, Any]]]
            .head("series").asInstanceOf[List[Map[String, Any]]]
            .head("values").asInstanceOf[List[List[Any]]]
        }.toOption
        values.getOrElse(List()).map { element =>
          val timestamp = dateFormat.parse(element.head.toString).getTime
          val memUsage = element(1).toString.toFloat
          val memUsageMB = memUsage * 9.313225746154785 * math.pow(10, -10) * 1024
          MemUsage(queryResponse.containerId, queryResponse.runConfig, timestamp, memUsageMB)
        }
    }
    sparkSession.sparkContext.parallelize(memoryUsageList)
      .toDS
  }

  def cleanPgFaultData(pgFaultData: Seq[QueryResponse]): Dataset[PgFaults] = {
    val pgFaultUsageList = pgFaultData.flatMap {
      queryResponse =>
        val values = Try {
          queryResponse.queryResponse("results").asInstanceOf[List[Map[String, Any]]]
            .head("series").asInstanceOf[List[Map[String, Any]]]
            .head("values").asInstanceOf[List[List[Any]]]
        }.toOption
        values.getOrElse(List()).map { element =>
          val timestamp = dateFormat.parse(element.head.toString).getTime
          val pgFaults = element(1).toString.toFloat
          PgFaults(queryResponse.containerId, queryResponse.runConfig, timestamp, pgFaults)
        }
    }
    sparkSession.sparkContext.parallelize(pgFaultUsageList)
      .toDS
  }

  def writeMemoryData(memoryData: DataFrame): Unit = {
    memoryData
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "containerName", "runConfiguration.*", "time", "" +
        "totalMemUsageMB", "swapMemUsageMB", "cacheMemUsageMB",
        "pgFaults", "majorPgFaults")
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("cadvisor-mem-timeseries-per-container"))
  }
}
