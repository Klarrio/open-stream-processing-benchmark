package evaluation.modes

import evaluation.config.EvaluationConfig
import evaluation.metrics.cadvisor.CadvisorResourceUtils
import evaluation.metrics.cadvisorextended.{CadvisorResourceComputer, HdfsResourceComputer, KafkaResourceComputer}
import evaluation.metrics.jmx.{JmxGCUtils, JmxMemoryUtils}
import evaluation.metrics.{LatencyUtils, ThroughputUtils}
import evaluation.utils.{MetricObservation, MetricUtils}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Analyzes high-throughput runs and therefore computes less detailed metrics on latency and throughput.
  * It needs to compute a lot of detailed metrics to identify the bottleneck of the job.
  * We also include metrics on HDFS and Kafka to check if they are not the bottleneck.
  *
  * @param data: data of the run to analyze
  * @param runTimes: dataframe which contains some key information on the run such as framework, start and end times, etc.
  * @param evaluationConfig: configuration object
  * @param sparkSession
  */
class ConstantRateEvaluator(data: Dataset[MetricObservation], runTimes: DataFrame, evaluationConfig: EvaluationConfig, sparkSession: SparkSession) {
  val metricUtils = new MetricUtils(sparkSession, evaluationConfig)
  val latencyUtils = new LatencyUtils(sparkSession, evaluationConfig)
  val throughputUtils = new ThroughputUtils(sparkSession, evaluationConfig)
  val jmxResourceUtils = new JmxMemoryUtils(runTimes, sparkSession, evaluationConfig)
  val jmxGcUtils = new JmxGCUtils(runTimes, sparkSession, evaluationConfig)
  val cadvisorResourceUtils = new CadvisorResourceUtils(runTimes, sparkSession, evaluationConfig)
  val cadvisorResourceComputer = new CadvisorResourceComputer(runTimes, sparkSession, evaluationConfig)
  val hdfsResourceComputer = new HdfsResourceComputer(runTimes, sparkSession, evaluationConfig)
  val kafkaResourceComputer = new KafkaResourceComputer(runTimes, sparkSession, evaluationConfig)

  def run(): Unit = {
    // Source: Cadvisor
    val cpuMetrics = cadvisorResourceUtils.processCpuMetricsAndComputeStats()
    cadvisorResourceUtils.processNetworkMetrics()
    cadvisorResourceUtils.processMemoryMetrics()

    cadvisorResourceComputer.wrangleCadvisorMetrics()
    hdfsResourceComputer.wrangleHdfsCadvisorMetrics()
    kafkaResourceComputer.wrangleKafkaCadvisorMetrics()

    // Source: JMX
    val jmxAggregatedMetrics = jmxResourceUtils.compute()
    jmxGcUtils.compute()

    // Source: Others
    val latency = latencyUtils.computeForSustainableThroughput(data)
    val throughput = throughputUtils.computeForSustainableTP(data)

    metricUtils.computeJobOverview(runTimes, latency, throughput, jmxAggregatedMetrics, cpuMetrics)
  }
}