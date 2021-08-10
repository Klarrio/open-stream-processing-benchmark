package evaluation.metrics.cadvisor

import java.net.URLEncoder
import java.text.{DateFormat, SimpleDateFormat}
import java.util.SimpleTimeZone

import evaluation.config.EvaluationConfig
import evaluation.metrics.jmx.TaskInfo
import evaluation.utils.{MemUsage, OperationTime, QueryResponse, RunConfiguration}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * - Disk IO wait time
  * - FS Usage
  * - FS IoTime
  * - FS WeightedIoTime
  * - FS WriteTime
  * - FS ReadTime
  */
class CadvisorDiskUtils(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import sparkSession.implicits._

  val runConfigColumns: Array[Column] = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
  val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))

  def getDataForTimeMetric(metricName: String, columnName: String, listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)]): DataFrame = {
    val diskData = listOfTaskInfo.map {
      case (taskInfo: TaskInfo, runConfiguration: RunConfiguration, condition: String) =>
        val queryString = s"SELECT derivative(value) from $metricName  WHERE value>0 AND container_name =~ /^*${taskInfo.containerId}/ $condition"
        logger.info(queryString)
        val encodedQuery = URLEncoder.encode(queryString, "UTF-8")
        val queryUrl = evaluationConfig.influxDbUrl + "/query?q=" + encodedQuery + "&db=cadvisor"
        val queryResponse = CadvisorQueryClient.getResponse(queryUrl)
        QueryResponse(taskInfo.containerId, taskInfo.taskName, runConfiguration, queryResponse)
    }.toList
    val cleanedDiskData = cleanTimeMetricData(diskData)
    cleanedDiskData
      .withColumnRenamed("operationTime", columnName)
  }

  def getDataForUsageMetric(metricName: String, columnName: String, listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)]): DataFrame = {
    val diskData = listOfTaskInfo.map {
      case (taskInfo: TaskInfo, runConfiguration: RunConfiguration, condition: String) =>
        val queryString = s"SELECT value from $metricName  WHERE value>0 AND container_name =~ /^*${taskInfo.containerId}/ $condition"
        logger.info(queryString)
        val encodedQuery = URLEncoder.encode(queryString, "UTF-8")
        val queryUrl = evaluationConfig.influxDbUrl + "/query?q=" + encodedQuery + "&db=cadvisor"
        val queryResponse = CadvisorQueryClient.getResponse(queryUrl)
        QueryResponse(taskInfo.containerId, taskInfo.taskName, runConfiguration, queryResponse)
    }.toList
    val cleanedDiskData = cleanUsageMetricData(diskData)
    cleanedDiskData
      .withColumnRenamed("mbits", columnName)
  }

  def cleanTimeMetricData(diskFiles: Seq[QueryResponse]): DataFrame = {
    val diskList = diskFiles.flatMap {
      queryResponse =>
        val values = Try {
          queryResponse.queryResponse("results").asInstanceOf[List[Map[String, Any]]]
            .head("series").asInstanceOf[List[Map[String, Any]]]
            .head("values").asInstanceOf[List[List[Any]]]
        }.toOption
        values.getOrElse(List()).map { element =>
          val timestamp = dateFormat.parse(element.head.toString).getTime
          val operationTime = element(1).toString.toLong
          OperationTime(queryResponse.containerId, queryResponse.runConfig, timestamp, operationTime)
        }
    }
    sparkSession.sparkContext.parallelize(diskList)
      .toDF()
  }

  def cleanUsageMetricData(diskFiles: Seq[QueryResponse]): DataFrame = {
    val diskList = diskFiles.flatMap {
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
    sparkSession.sparkContext.parallelize(diskList)
      .toDF()
  }

  def writeDiskData(cleanedDiskIOData: DataFrame): Unit = {
    cleanedDiskIOData
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "containerName", "containerTaskId", "runConfiguration.*", "time",
        "diskIOWait"
      )
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("disk-per-container-timeseries"))
  }
}
