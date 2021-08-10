package evaluation.metrics.cadvisor

import java.net.URLEncoder
import java.text.{DateFormat, SimpleDateFormat}
import java.util.SimpleTimeZone

import evaluation.config.EvaluationConfig
import evaluation.metrics.jmx.TaskInfo
import evaluation.utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

class CadvisorNetworkUtils(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import sparkSession.implicits._

  val runConfigColumns: Array[Column] = runTimes.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
  val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def getDataForBytesTransferredMetric(metricName: String, columnName: String, listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)]): DataFrame = {
    // Metric name is either 'rx' or 'tx'
    if (metricName != "rx" & metricName != "tx") sys.error("Network metric has to be rx or tx and here was provided:" + metricName)

    val bytesTransferredData = listOfTaskInfo.map {
      case (taskInfo: TaskInfo, runConfiguration: RunConfiguration, condition: String) =>
        val queryString = s"SELECT derivative(value) from ${metricName}_bytes  WHERE value>0 AND container_name =~ /^*${taskInfo.containerId}/ $condition"
        logger.info(queryString)
        val encodedQuery = URLEncoder.encode(queryString, "UTF-8")
        val queryUrl = evaluationConfig.influxDbUrl + "/query?q=" + encodedQuery + "&db=cadvisor"
        val queryResponse = CadvisorQueryClient.getResponse(queryUrl)
        QueryResponse(taskInfo.containerId, taskInfo.taskName, runConfiguration, queryResponse)
    }

    val bytesTransferredDF = cleanBytesTransferredData(bytesTransferredData)
      .withColumnRenamed("mbits", columnName)
    bytesTransferredDF
  }

  def getDataForPacketsDroppedMetric(metricName: String, columnName: String, listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)]): DataFrame = {
    // Metric name is either 'rx' or 'tx'
    if (metricName != "rx" & metricName != "tx") sys.error("Network metric has to be rx or tx and here was provided:" + metricName)

    val packetsDroppedData = listOfTaskInfo.map {
      case (taskInfo: TaskInfo, runConfiguration: RunConfiguration, condition: String) =>
        val queryString = s"SELECT derivative(value) from ${metricName}_dropped  WHERE value>0 AND container_name =~ /^*${taskInfo.containerId}/ $condition"
        logger.info(queryString)
        val encodedQuery = URLEncoder.encode(queryString, "UTF-8")
        val queryUrl = evaluationConfig.influxDbUrl + "/query?q=" + encodedQuery + "&db=cadvisor"
        val queryResponse = CadvisorQueryClient.getResponse(queryUrl)
        QueryResponse(taskInfo.containerId, taskInfo.taskName, runConfiguration, queryResponse)
    }
    val packetsDroppedDF = cleanPacketsDroppedData(packetsDroppedData)
      .withColumnRenamed("dropped", columnName)
    packetsDroppedDF
  }

  /**
    * Parse Json of the results
    */
  def cleanBytesTransferredData(resourceUsageFiles: Seq[QueryResponse]): DataFrame = {
    val dataString = resourceUsageFiles.flatMap { queryResponse =>
      val values = Try {
        queryResponse.queryResponse("results").asInstanceOf[List[Map[String, Any]]]
          .head("series").asInstanceOf[List[Map[String, Any]]]
          .head("values").asInstanceOf[List[List[Any]]]
      }.toOption
      values.getOrElse(List()).map { element =>
        dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
        val timestamp = dateFormat.parse(element.head.toString).getTime
        val num = element(1).toString.toFloat
        val rxMbits = num * 7.62939453125 * math.pow(10, -6)
        BytesTransferred(queryResponse.containerId, queryResponse.runConfig, timestamp, rxMbits.toLong)
      }
    }
    sparkSession.sparkContext.parallelize(dataString)
      .toDF()
  }

  /**
    * Parse Json of the results
    */
  def cleanPacketsDroppedData(resourceUsageFiles: Seq[QueryResponse]): DataFrame = {
    val dataString = resourceUsageFiles.flatMap { queryResponse =>
      val values = Try {
        queryResponse.queryResponse("results").asInstanceOf[List[Map[String, Any]]]
          .head("series").asInstanceOf[List[Map[String, Any]]]
          .head("values").asInstanceOf[List[List[Any]]]
      }.toOption
      values.getOrElse(List()).map { element =>
        dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
        val timestamp = dateFormat.parse(element.head.toString).getTime
        val num = element(1).toString.toLong
        PacketsDropped(queryResponse.containerId, queryResponse.runConfig, timestamp, num)
      }
    }
    sparkSession.sparkContext.parallelize(dataString)
      .toDF()
  }


  def writeNetworkData(networkDF: DataFrame): Unit = {
    networkDF
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "containerName", "runConfiguration.*", "time",
        "txMbits", "rxMbits",
        "txDropped", "rxDropped"
      )
      .sort("runConfigKey", "time")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("network-per-container-timeseries"))
  }
}
