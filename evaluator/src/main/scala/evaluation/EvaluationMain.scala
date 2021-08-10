package evaluation

import com.amazonaws.SDKGlobalConfiguration
import evaluation.config.EvaluationConfig
import evaluation.modes._
import evaluation.utils.IOUtils
import org.apache.spark.sql.SparkSession

/**
  * Main entrypoint of application.
  * Computes key metrics of a benchmark run and does pre-processing for visualization in Notebooks
  * - Initializes Spark
  * - Chooses the right evaluator based on the mode
  * - Computes all metrics for mode and writes them to S3
  *
  * There are seven modes:
  * - constant-rate: used for sustainable throughput workload and scalability workloads of OSPBench
  * - latency-constant-rate: used for latency workload of OSPBench
  * - single-burst: used for single burst at startup workload of OSPBench
  * - periodic-burst: used for workload with periodic bursts of OSPBench
  * - worker-failure: used for workload with worker failure of OSPBench
  * - master-failure: used for workload with master failure of OSPBench
  * - faulty-event: used for workload with job, stage or task failure of OSPBench
  */
object EvaluationMain {
  def main(args: Array[String]): Unit = {
    System.setProperty(SDKGlobalConfiguration.ENABLE_S3_SIGV4_SYSTEM_PROPERTY, "true")
    val sparkSession = initSpark
    val evaluationConfig = EvaluationConfig(sparkSession)

    // Configuration to read and write to S3
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", evaluationConfig.awsAccessKey)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", evaluationConfig.awsSecretKey)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", evaluationConfig.awsEndpoint)

    val ioUtils = new IOUtils(sparkSession, evaluationConfig)
    val (data, runTimes) = ioUtils.readDataAndComputeRunTimes()

    if (evaluationConfig.mode == "constant-rate")
      new ConstantRateEvaluator(data, runTimes, evaluationConfig, sparkSession).run()
    else if (evaluationConfig.mode == "latency-constant-rate")
      new LatencyConstantRateEvaluator(data, runTimes, evaluationConfig, sparkSession).run()
    else if (evaluationConfig.mode =="single-burst")
      new SingleBurstEvaluator(data, runTimes, evaluationConfig, sparkSession).run()
    else if (evaluationConfig.mode == "periodic-burst")
      new PeriodicBurstEvaluator(data, runTimes, evaluationConfig, sparkSession).run()
    else if (evaluationConfig.mode == "worker-failure")
      new WorkerFailureEvaluator(data, runTimes, evaluationConfig, sparkSession).run()
    else if (evaluationConfig.mode == "master-failure")
      new MasterFailureEvaluator(data, runTimes, evaluationConfig, sparkSession).run()
    else if (evaluationConfig.mode == "faulty-event")
      new FaultyEventEvaluator(data, runTimes, evaluationConfig, sparkSession).run()
  }

  def initSpark: SparkSession = {
    SparkSession.builder()
      .appName("benchmark-evaluator")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }
}
