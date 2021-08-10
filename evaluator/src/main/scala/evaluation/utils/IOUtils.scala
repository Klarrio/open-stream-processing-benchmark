package evaluation.utils

import evaluation.config.EvaluationConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Try

class IOUtils(sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {
  import sparkSession.implicits._

  /**
    * Reads in the data and computes the runtimes for the data
    * @return
    */
  def readDataAndComputeRunTimes(): (Dataset[MetricObservation], DataFrame) ={
    val rawData: DataFrame = sparkSession.read.json(evaluationConfig.dataFrameworkPath)
    val data: Dataset[MetricObservation] = formatFrameworkData(rawData)
      .select(col("runConfiguration"), col("inputKafkaTimestamp"), col("outputKafkaTimestamp"))
      .as[MetricObservation]
    data.cache()

    val runTimes = if(evaluationConfig.mode == "single-burst")
      computeRunTimesForSingleBurst(data.select("runConfiguration", "inputKafkaTimestamp", "outputKafkaTimestamp"))
    else computeRunTimes(data.select("runConfiguration", "inputKafkaTimestamp", "outputKafkaTimestamp"))

    runTimes.persist()
    (data, runTimes)
  }

  private def computeRunTimes(dataFrame: DataFrame): DataFrame = {
    // Compute when which framework was running with which configuration
    // To assign resource usage to the right configuration
    val runTimes = dataFrame
      .groupBy("runConfiguration")
      .agg(min("outputKafkaTimestamp").as("beginTime"),
        max("outputKafkaTimestamp").as("max_outputKafkaTimestamp"),
        max("inputKafkaTimestamp").as("max_inputKafkaTimestamp")
      ).withColumn("endTime", col("max_outputKafkaTimestamp"))
    runTimes
  }

  private def computeRunTimesForSingleBurst(dataFrame: DataFrame): DataFrame = {
    // Compute when which framework was running with which configuration
    // To assign resource usage to the right configuration
    val runTimes = dataFrame
      .select(col("runConfiguration"), col("runConfiguration.startTime").as("beginTime"))
      .distinct().withColumn("endTime", col("beginTime") + 10 * 60000)
    runTimes
  }

  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

  def formatFrameworkData(data: DataFrame): DataFrame = {
    // find the start time by taking the minimum start time over all containers
    // this is necessary since each worker has a different start time f.e. Kafka Streams
    val startTimeUDF = udf(startTimeExtractor)
    val minStartTime = data.limit(100).withColumn("startTime", startTimeUDF(col("key")))
      .select("startTime").agg(min("startTime")).collect()(0).getLong(0)

    // Split the filename to extract the configuration parameters of that run.
    val messageKeySplitterUDF = udf(messageKeySplitter)
    val dataWithConfigs = data.withColumn("runConfiguration", messageKeySplitterUDF(col("key"), lit(minStartTime)))

    dataWithConfigs
  }

  /**
    * Extracts config parameters from filename.
    */
  val messageKeySplitter: (String, Long) => Option[RunConfiguration] = {
    (str:String, startTime: Long) =>
      Try {
        val splitted = str.split("_")
        RunConfiguration(
          splitted(0).substring(splitted(0).lastIndexOf("/") + 1),
          splitted(2).replace("e", "").toInt,
          splitted(3).replace("v", "").toInt,
          splitted(4).replace("b", "").toInt,
          splitted(5).replace("s", "").toInt,
          splitted(6).replace("l", "").toInt,
          startTime
        )
      }.toOption
  }

  val startTimeExtractor: String => Long = {
    str =>
      val splitted = str.split("_")
      splitted(7).replace("t", "").toLong
  }

  // Check if the dataframe contains all the possible timestamp columns necessary, if not add empty column.
  def hasRequiredColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess
}
