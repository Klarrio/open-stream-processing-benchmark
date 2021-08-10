package evaluation.metrics

import evaluation.config.EvaluationConfig
import evaluation.utils.MetricObservation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ThroughputUtils(sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {

  def compute(metricsDF: Dataset[MetricObservation]): DataFrame = {
    val countAllRowsThatWereProcessed = metricsDF.groupBy("runConfiguration").count()
      .withColumnRenamed("runConfiguration", "runConfiguration3")

    val (inputThroughput, outputThroughput) = countPerSecond(metricsDF)
    val aggregatedThroughputMetrics = computeThroughput(inputThroughput, outputThroughput, countAllRowsThatWereProcessed)
    aggregatedThroughputMetrics
  }


  def computeForSustainableTP(metricsDF: Dataset[MetricObservation]): DataFrame = {
    val countAllRowsThatWereProcessed = metricsDF.groupBy("runConfiguration").count()

    countAllRowsThatWereProcessed
  }

  def computeForPeriodicBurst(metricsDF: Dataset[MetricObservation]) /*: (DataFrame, DataFrame)*/ = {

    metricsDF.groupBy("runConfiguration").count()
      .withColumnRenamed("runConfiguration", "runConfiguration3")

    countPerSecond(metricsDF)
  }


  def computeForWorkerFailure(metricsDF: Dataset[MetricObservation]) /*: (DataFrame, DataFrame)*/ = {

    metricsDF.groupBy("runConfiguration").count()
      .withColumnRenamed("runConfiguration", "runConfiguration3")

    countPerSecond(metricsDF)
  }

  def computeThroughput(inputThroughputDF: DataFrame, outputThroughputDF: DataFrame, countAllRows: DataFrame): DataFrame = {
    val overallAvgInputThroughput = inputThroughputDF.groupBy("runConfiguration").agg(avg("inputMsgCount").as("avgInputMsgCount"),
      max("inputMsgCount").as("maxInputMsgCount"),
      stddev("inputMsgCount").as("stddevInputMsgCount")
    )

    val overallAvgOutputThroughput = outputThroughputDF.groupBy("runConfiguration").agg(avg("outputMsgCount").as("avgOutputMsgCount"))
      .withColumnRenamed("runConfiguration", "runConfiguration2")

    // Join input and output throughput DataFrames back together
    val throughputDF = overallAvgInputThroughput.join(
      overallAvgOutputThroughput,
      overallAvgInputThroughput("runConfiguration") === overallAvgOutputThroughput("runConfiguration2")
    )

    throughputDF.join(
      countAllRows,
      throughputDF("runConfiguration") === countAllRows("runConfiguration3")
    )
      .drop("runConfiguration2").drop("runConfiguration3")
      .withColumn("outToInRatio", col("avgOutputMsgCount") / col("avgInputMsgCount"))

  }

  def countPerSecond(data: Dataset[MetricObservation]): (DataFrame, DataFrame) = {
    val runConfigColumns = data.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
    // To compute throughput in messages per second we bucketize our kafka input and output timestamp columns into second buckets
    val secondBucketizer = udf((timestamp: Long) => (Math.round(timestamp / 1000.0) * 1000).toString)
    val bucketizedDF = data.withColumn("outputBucket", secondBucketizer(col("outputKafkaTimestamp")))
      .withColumn("inputBucket", secondBucketizer(col("inputKafkaTimestamp")))

    // 1. Count messages in each bucket for input and output
    // 2. Compute the average amount of messages that came in vs. that came out of the framework
    val inputThroughputDF = bucketizedDF
      .groupBy("runConfiguration", "inputBucket")
      .agg(count("*").as("inputMsgCount"))
      .withColumn("inputBucketTime", col("inputBucket").cast(LongType))

    val outputThroughputDF = bucketizedDF
      .groupBy("runConfiguration", "outputBucket")
      .agg(count("*").as("outputMsgCount"))
      .withColumn("outputBucketTime", col("outputBucket").cast(LongType))


    /**
      * Writes away the throughput of one second buckets
      */
    outputThroughputDF
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "runConfiguration.*", "outputBucketTime", "outputBucket", "outputMsgCount")
      .orderBy("outputBucketTime")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("output-throughput-timeseries-second-buckets"))

    /**
      * Writes away the throughput of one second buckets
      */
    inputThroughputDF
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "runConfiguration.*", "inputBucketTime", "inputBucket", "inputMsgCount")
      .orderBy("inputBucketTime")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("input-throughput-timeseries-second-buckets"))

    (inputThroughputDF, outputThroughputDF)
  }
}
