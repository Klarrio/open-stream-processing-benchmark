package evaluation.metrics

import evaluation.config.EvaluationConfig
import evaluation.utils.MetricObservation
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class LatencyUtils(sparkSession: SparkSession, evaluationConfig: EvaluationConfig) {

  import sparkSession.implicits._

  def compute(stagesData: Dataset[MetricObservation]): DataFrame = {
    // Computation of the latency of the early stage observations:
    // latency = lastStageTimestamp - publishTimestamp
    val stagesTransformed = stagesData.map { observation =>
      val latency = observation.outputKafkaTimestamp - observation.inputKafkaTimestamp
      (observation.runConfiguration, "single", latency, observation.outputKafkaTimestamp, observation.inputKafkaTimestamp)
    }.toDF("runConfiguration", "latencyType", "latency", "outputKafkaTimestamp", "inputKafkaTimestamp")

    val minTimestamp = stagesTransformed.groupBy("runConfiguration").agg(min("outputKafkaTimestamp").as("startTime"))
    val latencyObservationsWithoutStartup = stagesTransformed
      .join(minTimestamp, "runConfiguration")
      .filter(col("outputKafkaTimestamp") > col("startTime") + 300000)
    latencyObservationsWithoutStartup.persist()

    /*Second bucketization and percentile computation*/
    computeSecondBucketMetrics(latencyObservationsWithoutStartup)

    /*Histogram bucketization*/
    precomputeDistributionPlot(stagesTransformed, latencyObservationsWithoutStartup, 3.0, "violin")

    val latencyResults = putResultInCsvFormat(latencyObservationsWithoutStartup)

    latencyResults
  }

  def computeForMasterFailure(stagesData: Dataset[MetricObservation]): DataFrame = {
    // Computation of the latency of the early stage observations:
    // latency = lastStageTimestamp - publishTimestamp
    val stagesTransformed = stagesData.map { observation =>
      val latency = observation.outputKafkaTimestamp - observation.inputKafkaTimestamp
      (observation.runConfiguration, "single", latency, observation.outputKafkaTimestamp, observation.inputKafkaTimestamp)
    }.toDF("runConfiguration", "latencyType", "latency", "outputKafkaTimestamp", "inputKafkaTimestamp")

    /*Second bucketization and percentile computation*/
    val latencyObservations = computeSecondBucketMetrics(stagesTransformed)

    /*Histogram bucketization*/
    precomputeDistributionPlot(stagesTransformed, stagesTransformed, 3.0, "violin")

    val latencyResults = putResultInCsvFormat(stagesTransformed)

    latencyResults
  }

  def computeForWorkerFailure(stagesData: Dataset[MetricObservation]) = {
    // Computation of the latency of the early stage observations:
    // latency = lastStageTimestamp - publishTimestamp
    val stagesTransformed = stagesData.map { observation =>
      val latency = observation.outputKafkaTimestamp - observation.inputKafkaTimestamp
      (observation.runConfiguration, "single", latency, observation.outputKafkaTimestamp, observation.inputKafkaTimestamp)
    }.toDF("runConfiguration", "latencyType", "latency", "outputKafkaTimestamp", "inputKafkaTimestamp")

    /*Second bucketization and percentile computation*/
    computeSecondBucketMetricsForWorkerFailure(stagesTransformed)
  }

  def computeForSustainableThroughput(stagesData: Dataset[MetricObservation]): DataFrame = {
    // Computation of the latency of the early stage observations:
    // latency = lastStageTimestamp - publishTimestamp
    val stagesTransformed = stagesData.map { observation =>
      val latency = observation.outputKafkaTimestamp - observation.inputKafkaTimestamp
      (observation.runConfiguration, "single", latency, observation.outputKafkaTimestamp, observation.inputKafkaTimestamp)
    }.toDF("runConfiguration", "latencyType", "latency", "outputKafkaTimestamp", "inputKafkaTimestamp")

    val minTimestamp = stagesTransformed.groupBy("runConfiguration").agg(min("outputKafkaTimestamp").as("startTime"))
    val latencyObservationsWithoutStartup = stagesTransformed
      .join(minTimestamp, "runConfiguration")
      .filter(col("outputKafkaTimestamp") > col("startTime") + 900000)

    val latencyResults = putResultInCsvFormatForSustainableThroughput(latencyObservationsWithoutStartup)
    computeSecondBucketMetricsForWorkerFailure(stagesTransformed)

    latencyResults
  }

  def computeForPeriodicBurst(stagesData: Dataset[MetricObservation]) = {
    // Computation of the latency of the early stage observations:
    // latency = lastStageTimestamp - publishTimestamp
    val stagesTransformed = stagesData.map { observation =>
      val latency = observation.outputKafkaTimestamp - observation.inputKafkaTimestamp
      (observation.runConfiguration, "single", latency, observation.outputKafkaTimestamp, observation.inputKafkaTimestamp)
    }.toDF("runConfiguration", "latencyType", "latency", "outputKafkaTimestamp", "inputKafkaTimestamp")

    val minTimestamp = stagesTransformed.groupBy("runConfiguration").agg(min("outputKafkaTimestamp").as("startTime"))
    val latencyObservationsWithoutStartup = stagesTransformed
      .join(minTimestamp, "runConfiguration")
      .filter(col("outputKafkaTimestamp") > col("startTime") + 300000)

    /*Second bucketization and percentile computation*/
    computeSecondBucketMetrics(latencyObservationsWithoutStartup)
  }

  /**
    * Groups on last stage, amount of runs, publish interval, volume, amt of cores, short look-back, long look-back and latencyType
    * Computes mean, stddev, min, max, p5, p50, p75, p95, p99
    *
    * @param completeMetricsFrame
    * @return
    */
  def putResultInCsvFormat(completeMetricsFrame: DataFrame): DataFrame = {
    completeMetricsFrame.createTempView("completeMetricsFrame")
    sparkSession.sql(
      s"""SELECT runConfiguration,
         				|latencyType,
         				|format_number(avg(latency), 3) as avgLatency,
         				|format_number(stddev(latency), 3) as stddevLatency,
         				|min(latency) as  minLatency,
         				|max(latency) as maxLatency,
         |percentile_approx(latency,0.01) as p1,
         				|percentile_approx(latency,0.05) as p5,
         				|percentile_approx(latency,0.25) as p25,
         				|percentile_approx(latency,0.5) as p50,
         				|percentile_approx(latency,0.75) as p75,
         				|percentile_approx(latency,0.95) as p95,
         |percentile_approx(latency,0.99) as p99,
         |percentile_approx(latency,0.999) as p999,
         |percentile_approx(latency,0.99999) as p99999
         				|
         				|FROM completeMetricsFrame
         				|
         				|GROUP BY runConfiguration,
         				|latencyType
         				|
         				|ORDER BY runConfiguration,
         				|latencyType""".stripMargin
    )
  }

  /**
    * Groups on last stage, amount of runs, publish interval, volume, amt of cores, short look-back, long look-back and latencyType
    * Computes mean, stddev, min, max, p5, p50, p75, p95, p99
    *
    * @param completeMetricsFrame
    * @return
    */
  def putResultInCsvFormatForSustainableThroughput(completeMetricsFrame: DataFrame): DataFrame = {
    completeMetricsFrame.createTempView("completeMetricsFrame")
    sparkSession.sql(
      s"""SELECT runConfiguration,
         				|latencyType,
         				|percentile_approx(latency,0.5) as p50,
                |percentile_approx(latency,0.99) as p99
         				|
         				|FROM completeMetricsFrame
         				|
         				|GROUP BY runConfiguration,
         				|latencyType
         				|
         				|ORDER BY runConfiguration,
         				|latencyType""".stripMargin
    )
  }

  def computeSecondBucketMetrics(latencyObservationsWithoutStartup: DataFrame): DataFrame = {
    val runConfigColumns = latencyObservationsWithoutStartup.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
    val secondBucketizer = udf((timestamp: Long) => (Math.round(timestamp / 1000.0) * 1000).toString)
    val phaseStringifier = udf((phase: Int) => {
      val lastStageMapper = Map(0 -> "ingest", 1 -> "parse", 2 -> "join", 3 -> "aggregate", 4 -> "window")
      lastStageMapper.get(phase)
    })

    val outputBucketWindow = Window.partitionBy("outputBucket")
    val percentile_01_second = expr("percentile_approx (latency, 0.01)").over(outputBucketWindow)
    val percentile_25_second = expr("percentile_approx (latency, 0.25)").over(outputBucketWindow)
    val percentile_50_second = expr("percentile_approx (latency, 0.5)").over(outputBucketWindow)
    val percentile_75_second = expr("percentile_approx (latency, 0.75)").over(outputBucketWindow)
    val percentile_95_second = expr("percentile_approx (latency, 0.95)").over(outputBucketWindow)
    val percentile_99_second = expr("percentile_approx (latency, 0.99)").over(outputBucketWindow)
    val percentile_999_second = expr("percentile_approx (latency, 0.999)").over(outputBucketWindow)
    val percentile_99999_second = expr("percentile_approx (latency, 0.99999)").over(outputBucketWindow)
    val avg_second = expr("avg(latency)").over(outputBucketWindow)
    val stddev_second = expr("stddev(latency)").over(outputBucketWindow)

    val bucketizedDF = latencyObservationsWithoutStartup.withColumn("outputBucket", secondBucketizer(col("outputKafkaTimestamp")))
      .withColumn("inputBucket", secondBucketizer(col("inputKafkaTimestamp")))
      .withColumn("outputBucketTime", col("outputBucket").cast(LongType))
      .withColumn("inputBucketTime", col("inputBucket").cast(LongType))
      .withColumn("percentile_01_second", percentile_01_second)
      .withColumn("percentile_25_second", percentile_25_second)
      .withColumn("percentile_50_second", percentile_50_second)
      .withColumn("percentile_75_second", percentile_75_second)
      .withColumn("percentile_95_second", percentile_95_second)
      .withColumn("percentile_99_second", percentile_99_second)
      .withColumn("percentile_999_second", percentile_999_second)
      .withColumn("percentile_99999_second", percentile_99999_second)
      .withColumn("avg_latency_second", avg_second)
      .withColumn("stddev_latency_second", stddev_second)
      .select("runConfiguration", "outputBucketTime", "percentile_01_second", "percentile_25_second", "percentile_50_second", "percentile_75_second", "percentile_95_second", "percentile_99_second", "percentile_999_second", "percentile_99999_second", "avg_latency_second", "stddev_latency_second")
      .distinct()


    bucketizedDF
      .orderBy("outputBucketTime")
      .withColumn("last_stage", phaseStringifier(col("runConfiguration.phase")))
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "runConfiguration.*", "last_stage", "outputBucketTime", "percentile_01_second", "percentile_25_second", "percentile_50_second", "percentile_75_second", "percentile_95_second", "percentile_99_second", "percentile_999_second", "percentile_99999_second", "avg_latency_second", "stddev_latency_second")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("latency-timeseries-data-without-startup"))

    bucketizedDF
  }

  def computeSecondBucketMetricsForWorkerFailure(latencyObservationsWithoutStartup: DataFrame): DataFrame = {
    val runConfigColumns = latencyObservationsWithoutStartup.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)
    val secondBucketizer = udf((timestamp: Long) => (Math.round(timestamp / 1000.0) * 1000).toString)
    val phaseStringifier = udf((phase: Int) => {
      val lastStageMapper = Map(0 -> "ingest", 1 -> "parse", 2 -> "join", 3 -> "aggregate", 4 -> "window")
      lastStageMapper.get(phase)
    })

    val outputBucketWindow = Window.partitionBy("outputBucket")
    val percentile_50_second = expr("percentile_approx (latency, 0.5, 100)").over(outputBucketWindow)
    val percentile_99_second = expr("percentile_approx (latency, 0.99, 100)").over(outputBucketWindow)

    val bucketizedDF = latencyObservationsWithoutStartup.withColumn("outputBucket", secondBucketizer(col("outputKafkaTimestamp")))
      .withColumn("outputBucketTime", col("outputBucket").cast(LongType))
      .withColumn("percentile_50_second", percentile_50_second)
      .withColumn("percentile_99_second", percentile_99_second)
      .select("runConfiguration", "outputBucketTime", "percentile_50_second", "percentile_99_second")
      .distinct()

    bucketizedDF
      .orderBy("outputBucketTime")
      .withColumn("last_stage", phaseStringifier(col("runConfiguration.phase")))
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "runConfiguration.*", "last_stage", "outputBucketTime", "percentile_50_second", "percentile_99_second")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("latency-timeseries-data-without-startup"))

    bucketizedDF
  }

  def precomputeDistributionPlot(latencyObservations: DataFrame, latencyObservationsWithoutStartup: DataFrame, divisor: Double, plotType: String): Unit = {
    val runConfigColumns = latencyObservations.select("runConfiguration.*").columns.map("runConfiguration." + _).map(col)

    latencyObservations.withColumn("latencyBucket", (round(col("latency") / divisor) * divisor).cast(StringType))
      .groupBy("runConfiguration", "latencyBucket")
      .agg(count("*").alias("latencyBucketCount"))
      .withColumn("latencyBucket", col("latencyBucket").cast(IntegerType))
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "runConfiguration.*", "latencyBucket", "latencyBucketCount")
      .orderBy("runConfigKey", "latencyBucket")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("latency-" + plotType + "-with-startup-data"))


    latencyObservationsWithoutStartup.withColumn("latencyBucket", (round(col("latency") / divisor) * divisor).cast(StringType))
      .groupBy("runConfiguration", "latencyBucket")
      .agg(count("*").alias("latencyBucketCount"))
      .withColumn("latencyBucket", col("latencyBucket").cast(IntegerType))
      .withColumn("runConfigKey", concat_ws("-", runConfigColumns: _*))
      .select("runConfigKey", "runConfiguration.*", "latencyBucket", "latencyBucketCount")
      .orderBy("runConfigKey", "latencyBucket")
      .coalesce(1)
      .write
      .partitionBy("runConfigKey")
      .option("header", "true")
      .csv(evaluationConfig.dataOutputPath("latency-" + plotType + "-without-startup-data"))
  }
}
