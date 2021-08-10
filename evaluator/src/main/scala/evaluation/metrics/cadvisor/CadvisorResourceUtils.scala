package evaluation.metrics.cadvisor

import java.sql.Timestamp

import evaluation.config.EvaluationConfig
import evaluation.metrics.jmx.TaskInfo
import evaluation.utils.RunConfiguration
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try
import scala.util.parsing.json.JSON

class CadvisorResourceUtils(runTimes: DataFrame, sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {

  import sparkSession.implicits._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val mesosTaskHistories: Map[String, Any] = requestMesosTaskHistories()

  val timeConditions: Array[(RunConfiguration, Long, Long, String)] = runTimes.select("runConfiguration", "beginTime", "endTime")
    .as[(RunConfiguration, Long, Long)]
    .rdd
    .map {
      case (runConfig: RunConfiguration, timestamp1Long: Long, timestamp2Long: Long) =>
        val timestamp1 = new Timestamp(timestamp1Long)
        val timestamp2 = new Timestamp(timestamp2Long)
        (runConfig, timestamp1Long, timestamp2Long, "and time > '" + timestamp1.toString + "' and time < '" + timestamp2.toString + "'")
    }.collect()

  val listOfTaskInfo: Seq[(TaskInfo, RunConfiguration, String)] = evaluationConfig.listOfTaskNames.flatMap { taskName =>
    timeConditions.flatMap { case (runConfig, startTime, endTime, condition) =>
      extractTaskInfo(mesosTaskHistories, taskName, startTime, endTime)
        .map(info => (info, runConfig, condition))
    }
  }
  logger.info(runTimes.toString())


  def processCpuMetricsAndComputeStats(): DataFrame = {
    val cadvisorCpuUtils = new CadvisorCpuUtils(runTimes, sparkSession, evaluationConfig)
    // CPU total
    val cpuTotalDF = cadvisorCpuUtils.getDataForMetric("cpu_usage_total", "cpu", listOfTaskInfo)
    // CPU user
    val cpuUserDF = cadvisorCpuUtils.getDataForMetric("cpu_usage_user", "userCpu", listOfTaskInfo)
    // CPU system
    val cpuSystemDF = cadvisorCpuUtils.getDataForMetric("cpu_usage_system", "systemCpu", listOfTaskInfo)

    val cpuStats = cpuTotalDF.join(cpuUserDF, Seq("containerName", "containerTaskId", "runConfiguration", "time"), "outer")
      .join(cpuSystemDF, Seq("containerName", "containerTaskId", "runConfiguration", "time"), "outer")

    cadvisorCpuUtils.writeCleanedCpuData(cpuStats)

    val cpuTotalMetrics: DataFrame = cadvisorCpuUtils.computeMetrics(cpuStats)
    cpuTotalMetrics
  }

  def processNetworkMetrics(): Unit = {
    val cadvisorNetworkUtils = new CadvisorNetworkUtils(runTimes, sparkSession, evaluationConfig)
    // bytes transferred: received and transmitted
    val rxBytesTransferredDF = cadvisorNetworkUtils.getDataForBytesTransferredMetric("rx", "rxMbits", listOfTaskInfo)
    val txBytesTransferredDF = cadvisorNetworkUtils.getDataForBytesTransferredMetric("tx", "txMbits", listOfTaskInfo)

    // packets dropped: received and transmitted
    val rxPacketsDroppedDF = cadvisorNetworkUtils.getDataForPacketsDroppedMetric("rx", "rxDropped", listOfTaskInfo)
    val txPacketsDroppedDF = cadvisorNetworkUtils.getDataForPacketsDroppedMetric("tx", "txDropped", listOfTaskInfo)

    val networkMetrics = rxBytesTransferredDF.join(txBytesTransferredDF, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(rxPacketsDroppedDF, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(txPacketsDroppedDF, Seq("containerName", "runConfiguration", "time"), "outer")

    cadvisorNetworkUtils.writeNetworkData(networkMetrics)
  }

  def processMemoryMetrics(): Unit = {
    val cadvisorMemoryUtils = new CadvisorMemoryUtils(runTimes, sparkSession, evaluationConfig)
    // usage
    val totalMemoryUsage = cadvisorMemoryUtils.getDataForBytesMetric("memory_usage", "totalMemUsageMB", listOfTaskInfo)
    // swap
    val swapMemoryUsage = cadvisorMemoryUtils.getDataForBytesMetric("memory_swap", "swapMemUsageMB", listOfTaskInfo)
    // cache
    val cacheMemoryUsage = cadvisorMemoryUtils.getDataForBytesMetric("memory_cache", "cacheMemUsageMB", listOfTaskInfo)

    // page faults
    val pgFaults = cadvisorMemoryUtils.getDataForPageFaultMetric("memory_pgfault", "pgFaults", listOfTaskInfo)
    // major page faults
    val majorPgFaults = cadvisorMemoryUtils.getDataForPageFaultMetric("memory_pgmajfault", "majorPgFaults", listOfTaskInfo)

    val memoryMetrics = totalMemoryUsage.join(swapMemoryUsage, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(cacheMemoryUsage, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(pgFaults, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(majorPgFaults, Seq("containerName", "runConfiguration", "time"), "outer")

    cadvisorMemoryUtils.writeMemoryData(memoryMetrics)
  }

  def processDiskMetrics(): Unit = {
    val cadvisorDiskUtils = new CadvisorDiskUtils(runTimes, sparkSession, evaluationConfig)

    // Disk IO wait time
    val diskIoWaitTime = cadvisorDiskUtils.getDataForTimeMetric("disk_io_wait", "DiskIoTime", listOfTaskInfo)

    // FS Usage
    val fsUsage = cadvisorDiskUtils.getDataForUsageMetric("fs_usage", "FsUsageMB", listOfTaskInfo)

    // FS IoTime
    val fsIoTime = cadvisorDiskUtils.getDataForTimeMetric("fs_io_time", "FsIoTime", listOfTaskInfo)
    // FS WeightedIoTime
    val fsWeightedIoTime = cadvisorDiskUtils.getDataForTimeMetric("fs_weighted_io_time", "FsWeightedIoTime", listOfTaskInfo)

    // FS WriteTime
    val fsWriteTime = cadvisorDiskUtils.getDataForTimeMetric("fs_write_time", "FsWriteTime", listOfTaskInfo)
    // FS ReadTime
    val fsReadTime = cadvisorDiskUtils.getDataForTimeMetric("fs_read_time", "FsReadTime", listOfTaskInfo)

    val diskMetrics = diskIoWaitTime.join(fsUsage, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(fsIoTime, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(fsWeightedIoTime, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(fsWriteTime, Seq("containerName", "runConfiguration", "time"), "outer")
      .join(fsReadTime, Seq("containerName", "runConfiguration", "time"), "outer")

    cadvisorDiskUtils.writeDiskData(diskMetrics)
  }

  // Returns a map containing the frameworks info of Mesos
  // From this map the container names will be extracted
  private def requestMesosTaskHistories(): Map[String, Any] = {
    val url = evaluationConfig.clusterUrl + "/mesos/tasks"
    val httpGet = new HttpGet(url)

    // set the desired header values
    // To get token:
    // dcos config show core.dcos_acs_token
    httpGet.setHeader("Authorization", "token=" + evaluationConfig.dcosAccessToken)
    // execute the request
    val client = new DefaultHttpClient
    val response = client.execute(httpGet)

    val in = response.getEntity
    val encoding = response.getEntity.getContentEncoding

    val result = EntityUtils.toString(in, "UTF-8")
    client.getConnectionManager.shutdown()

    val parsedResult = JSON.parseFull(result).get.asInstanceOf[Map[String, Any]]
    logger.info(result.toString)
    parsedResult
  }

  def extractTaskInfo(parsedResult: Map[String, Any], taskName: String, startTime: Long, endTime: Long): List[TaskInfo] = {
    logger.info(s"""extracting task info for ${taskName} + start: ${startTime} + endTime: ${endTime}""")
    val taskInfos = parsedResult("tasks")
      .asInstanceOf[List[Map[String, Any]]]
      .filter { taskJson =>
        taskJson("name").toString.contains(taskName)
      }
      .flatMap { taskJson =>
        Try {
          val taskName = taskJson("name").toString
          val statuses = taskJson("statuses").asInstanceOf[List[Map[String, Any]]]
          logger.info(taskJson.toString)

          val taskStart = statuses.find {
            statusMap =>
              statusMap("state").asInstanceOf[String].contains("TASK_STARTING")
          }.get
          logger.info(taskStart.toString)
          val taskStartTimeMillis = Math.round(taskStart("timestamp").asInstanceOf[Double] * 1000).toLong

          val taskEnd = statuses.find {
            statusMap =>
              statusMap("state").asInstanceOf[String].contains("TASK_KILLED")
          }.getOrElse(Map("timestamp" -> System.currentTimeMillis() / 1000.0)) // For spark the container will still be running
          logger.info(taskEnd.toString())

          val taskEndTimeMillis = Math.round(taskEnd("timestamp").asInstanceOf[Double] * 1000).toLong

          val containerId = taskStart("container_status")
            .asInstanceOf[Map[String, Any]]("container_id")
            .asInstanceOf[Map[String, Any]]("value").toString

          val taskInfo = TaskInfo(taskName, containerId, taskStartTimeMillis, taskEndTimeMillis)
          logger.info(taskInfo.toString)
          taskInfo
        }.toOption
      }.filter { taskInfo =>
      // Only use the ones which have the right task name and which were running at the time the latency events were gathered
      // We use a buffer of 5 minutes because for frameworks like Kafka Streams some containers may lag behind on others leading to events arriving earlier than some the startup time of some threads.
      (taskInfo.taskName.contains(taskName) && taskInfo.startTime - 300000 < startTime && taskInfo.endTime + 300000 > endTime) ||
        // for worker failure we also need to catch containers that stopped too early so we look at all containers that were running in the beginning AND the end of the workload
        (taskInfo.taskName.contains(taskName) && taskInfo.startTime - 300000 < startTime && taskInfo.endTime + 25 * 60 * 1000 > endTime) ||
        (taskInfo.taskName.contains(taskName) && taskInfo.startTime - 25 * 60 * 1000 < startTime && taskInfo.endTime + 300000 > endTime)
    }
    logger.info(taskInfos.toString)
    taskInfos
  }
}