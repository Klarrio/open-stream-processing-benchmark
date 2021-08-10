package evaluation.utils

import evaluation.config.EvaluationConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


class MetricUtils(sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {


  def computeJobOverviewConstantRate(runTimes: DataFrame,
    latencyResults: DataFrame,
    jmxAggregatedMetrics: DataFrame,
    cadvisorAggregatedMetrics: DataFrame): Unit = {
    val results = runTimes.withColumn("amtWorkers", lit(evaluationConfig.amountOfWorkers))
      .withColumn("cpuPerWorker", lit(evaluationConfig.cpuPerWorker))
      .withColumn("memPerWorker", lit(evaluationConfig.memoryPerWorker))
      .join(latencyResults, Seq("runConfiguration"), "fullouter")
      .join(jmxAggregatedMetrics, Seq("runConfiguration"), "fullouter")
      .join(cadvisorAggregatedMetrics, Seq("runConfiguration"), "fullouter")
      .select(
        col("runConfiguration.framework").as("framework"),
        col("runConfiguration.phase").as("phase"),
        col("runConfiguration.scale").as("scale"),
        col("runConfiguration.bufferTimeout").as("bufferTimeout"),
        col("runConfiguration.shortLb").as("shortLb"),
        col("runConfiguration.longLb").as("longLb"),
        col("runConfiguration.startTime").as("startTime"),
        col("*")
      )
      .drop("runConfiguration")

    results
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("results"))
  }

  def computeJobOverview(runTimes: DataFrame,
    latencyResults: DataFrame,
    throughputResults: DataFrame,
    jmxAggregatedMetrics: DataFrame,
    cadvisorAggregatedMetrics: DataFrame): Unit = {
    val results = runTimes.withColumn("amtWorkers", lit(evaluationConfig.amountOfWorkers))
      .withColumn("cpuPerWorker", lit(evaluationConfig.cpuPerWorker))
      .withColumn("memPerWorker", lit(evaluationConfig.memoryPerWorker))
      .join(latencyResults, Seq("runConfiguration"), "fullouter")
      .join(throughputResults, Seq("runConfiguration"), "fullouter")
      .join(jmxAggregatedMetrics, Seq("runConfiguration"), "fullouter")
      .join(cadvisorAggregatedMetrics, Seq("runConfiguration"), "fullouter")
      .select(
        col("runConfiguration.framework").as("framework"),
        col("runConfiguration.phase").as("phase"),
        col("runConfiguration.scale").as("scale"),
        col("runConfiguration.bufferTimeout").as("bufferTimeout"),
        col("runConfiguration.shortLb").as("shortLb"),
        col("runConfiguration.longLb").as("longLb"),
        col("runConfiguration.startTime").as("startTime"),
        col("*")
      )
      .drop("runConfiguration")

    results
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("results"))
  }

  def computeSingleBurstJobOverview(throughputResults: DataFrame,
    jmxAggregatedMetrics: DataFrame,
    cadvisorAggregatedMetrics: DataFrame): Unit = {
    val results = throughputResults
      .join(jmxAggregatedMetrics, Seq("runConfiguration"), "fullouter")
      .join(cadvisorAggregatedMetrics, Seq("runConfiguration"), "fullouter")
      .select(
        col("runConfiguration.framework").as("framework"),
        col("runConfiguration.phase").as("phase"),
        col("runConfiguration.scale").as("scale"),
        col("runConfiguration.bufferTimeout").as("bufferTimeout"),
        col("runConfiguration.shortLb").as("shortLb"),
        col("runConfiguration.longLb").as("longLb"),
        col("runConfiguration.startTime").as("startTime"),
        col("*")
      )
      .withColumn("amtWorkers", lit(evaluationConfig.amountOfWorkers))
      .withColumn("cpuPerWorker", lit(evaluationConfig.cpuPerWorker))
      .withColumn("memPerWorker", lit(evaluationConfig.memoryPerWorker))
      .drop("runConfiguration")

    results
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("results"))
  }
}
