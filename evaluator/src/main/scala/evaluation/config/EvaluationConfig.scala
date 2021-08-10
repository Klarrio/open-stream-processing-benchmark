package evaluation.config

import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

case class EvaluationConfig(sparkSession: SparkSession) extends Serializable {

  val systemTime: String = new Timestamp(System.currentTimeMillis()).toString.replaceAll(" ", "_").replaceAll(":", "_").replaceAll("\\.", "_")

  // get configurations from the spark submit arguments
  val framework: String = sparkSession.conf.get("spark.FRAMEWORK")
  val clusterUrl: String = sparkSession.conf.get("spark.CLUSTER_URL")
  val influxDbUrl: String = sparkSession.conf.get("spark.INFLUXDB_URL")
  val dcosAccessToken: String = sparkSession.conf.get("spark.DCOS_ACCESS_TOKEN")
  val mode: String = sparkSession.conf.get("spark.MODE")
  val jobuuid: String = sparkSession.conf.get("spark.FILEPATH")
  val lastStage: String = sparkSession.conf.get("spark.LAST_STAGE")
  val beginTime: String = sparkSession.conf.get("spark.BEGINTIME")
  val inputMetricsPath: String = sparkSession.conf.get("spark.INPUT_METRICS_PATH")
  val resultsPath: String = sparkSession.conf.get("spark.RESULTS_PATH")

  val memoryPerWorker: String = sparkSession.conf.get("spark.WORKER_MEM")
  val cpuPerWorker: Int = sparkSession.conf.get("spark.WORKER_CPU").toInt
  val amountOfWorkers: Int = sparkSession.conf.get("spark.AMT_WORKERS").toInt
  val totalCpuResources: Int = cpuPerWorker*amountOfWorkers

  // Config file variables
  lazy val configProperties: Config = ConfigFactory.load("resources.conf")
  lazy val awsEndpoint: String = configProperties.getString("aws.endpoint")
  lazy val awsAccessKey: String = sparkSession.conf.get("spark.AWS_ACCESS_KEY")
  lazy val awsSecretKey: String = sparkSession.conf.get("spark.AWS_SECRET_KEY")

  lazy val dataFrameworkPath: String = inputMetricsPath  + "/" + framework + "/" + mode + "/observations-log-" + jobuuid + "*"
  lazy val resourceMetricsFrameworkPath: String = inputMetricsPath + "/"  + framework + "/" + mode + "/metrics-log-" + jobuuid + "*"
  lazy val gcMetricsFrameworkPath: String = inputMetricsPath + "/"  + framework + "/" + mode + "/gc-log-" + jobuuid + "*"
  lazy val cadvisorMetricsFrameworkPath: String = inputMetricsPath + "/"  + framework + "/" + mode + "/cadvisor-log-" + jobuuid + "*"
  lazy val cadvisorHdfsMetricsFrameworkPath: String = inputMetricsPath  + "/" + framework + "/" + mode + "/hdfs-cadvisor-log-" + jobuuid + "*"
  lazy val cadvisorKafkaMetricsFrameworkPath: String = inputMetricsPath + "/"  + framework + "/" + mode + "/kafka-cadvisor-log-" + jobuuid + "*"

  def dataOutputPath(dataContent: String): String = {
    resultsPath + "/" + framework + "/" + mode + "/stage" + lastStage + "/" + amountOfWorkers + "x-" + cpuPerWorker + "cpu-" + memoryPerWorker + "gb/" + beginTime + "-" + jobuuid + "/" + dataContent + ".csv"
  }


  val listOfTaskNames: List[String] = if (framework.contains("FLINK")) {
    List("flink-jobmanager") ++ (1 to amountOfWorkers).map{ el => "flink-taskmanager-" + el}.toList
  } else if (framework.contains("KAFKASTREAMS")) {
    (1 to amountOfWorkers).map{ el => "kafka-thread-" + el}.toList
  } else {
    List("spark-master") ++ (1 to amountOfWorkers).map{ el => "spark-worker-" + el}.toList
  }
}
