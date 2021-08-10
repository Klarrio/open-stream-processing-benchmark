package evaluation.modes

import evaluation.config.EvaluationConfig
import evaluation.metrics.cadvisor.CadvisorResourceUtils
import evaluation.metrics.cadvisorextended.CadvisorResourceComputer
import evaluation.metrics.jmx.{JmxGCUtils, JmxMemoryUtils}
import evaluation.metrics.{LatencyUtils, ThroughputUtils}
import evaluation.utils.{MetricObservation, MetricUtils}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Computes metrics that are important to analyze the periodic burst workload.
  * The focus here is mainly on timeseries data because we focus on the behavior throughout a burst.
  *
  * @param data: data of the run to analyze
  * @param runTimes: dataframe which contains some key information on the run such as framework, start and end times, etc.
  * @param evaluationConfig: configuration object
  * @param sparkSession
  */
class PeriodicBurstEvaluator(data: Dataset[MetricObservation], runTimes: DataFrame, evaluationConfig: EvaluationConfig, sparkSession: SparkSession) {
  val metricUtils = new MetricUtils(sparkSession, evaluationConfig)
  val latencyUtils = new LatencyUtils(sparkSession, evaluationConfig)
  val throughputUtils = new ThroughputUtils(sparkSession, evaluationConfig)
  val jmxResourceUtils = new JmxMemoryUtils(runTimes, sparkSession, evaluationConfig)
  val jmxGcUtils = new JmxGCUtils(runTimes, sparkSession, evaluationConfig)
  val cadvisorResourceUtils = new CadvisorResourceUtils(runTimes, sparkSession, evaluationConfig)

  def run(): Unit = {
    // Source: Cadvisor
    cadvisorResourceUtils.processCpuMetricsAndComputeStats()
    cadvisorResourceUtils.processNetworkMetrics()
    cadvisorResourceUtils.processMemoryMetrics()

    // Source: JMX
    jmxResourceUtils.compute()
    jmxGcUtils.compute()

    // Source: Others
    latencyUtils.computeForPeriodicBurst(data)
    throughputUtils.computeForPeriodicBurst(data)
  }
}