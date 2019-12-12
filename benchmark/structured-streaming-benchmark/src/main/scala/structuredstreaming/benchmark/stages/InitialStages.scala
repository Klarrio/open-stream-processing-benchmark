package structuredstreaming.benchmark.stages

import java.sql.Timestamp
import java.time.Instant

import common.benchmark.stages.InitialStagesTemplate
import common.config.LastStage._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import structuredstreaming.benchmark.BenchmarkSettingsForStructuredStreaming

/**
  * Contains all methods required for parsing and joining the incoming streams
  *
  * - Reads in from Kafka
  * - Parse JSON data to Dataframe
  * - Join the flow and speed observations together for each timestamp, measurement ID and lane ID
  *
  * @param sparkSession
  * @param settings settings of the job
  */
class InitialStages(sparkSession: SparkSession, settings: BenchmarkSettingsForStructuredStreaming)
  extends Serializable with InitialStagesTemplate {

  import sparkSession.implicits._

  /**
    * Consumes from Kafka from flow topics
    *
    * @return DataFrames for speed and flow data
    */
  def ingestStage(): (DataFrame, DataFrame) = {
    val flowStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", settings.general.kafkaBootstrapServers)
      .option("subscribe", settings.general.flowTopic)
      .option("startingOffsets", settings.general.kafkaAutoOffsetReset)
      .option("includeTimestamp", true)
      .load()

    val speedStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", settings.general.kafkaBootstrapServers)
      .option("subscribe", settings.general.speedTopic )
      .option("startingOffsets", settings.general.kafkaAutoOffsetReset)
      .option("includeTimestamp", true)
      .load()

    (flowStream, speedStream)
  }

  /**
    * Parses the data
    *
    * @return parsed Dataframes for speed and flow
    */
  def parsingStage(rawFlowStream: DataFrame, rawSpeedStream: DataFrame): (DataFrame, DataFrame) = {

    val flowSchema = StructType(Seq(
      StructField("lat", DoubleType, true), StructField("long", DoubleType, true), StructField("flow", IntegerType, true),
      StructField("period", IntegerType, true), StructField("accuracy", IntegerType, true), StructField("timestamp", StringType, true),
      StructField("num_lanes", IntegerType, true)
    ))
    val speedSchema = StructType(Seq(
      StructField("lat", DoubleType, true), StructField("long", DoubleType, true), StructField("speed", DoubleType, true),
      StructField("accuracy", IntegerType, true), StructField("timestamp", StringType, true),
      StructField("num_lanes", IntegerType, true)
    ))


    val flowStream = rawFlowStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(
        split($"key", "/lane").getItem(0).as("measurementId"),
        concat(lit("lane"), split($"key", "/lane").getItem(1)).as("internalId"),
        from_json($"value", flowSchema).as("flowObservation"),
        col("timestamp").as("flowPublishTimestamp"),
        current_timestamp().as("flowIngestTimestamp")
      ).withColumn("timestamp", unix_timestamp($"flowObservation.timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

    val speedStream = rawSpeedStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(
        split($"key", "/lane").getItem(0).as("measurementId"),
        concat(lit("lane"), split($"key", "/lane").getItem(1)).as("internalId"),
        from_json($"value", speedSchema).as("speedObservation"),
        col("timestamp").as("speedPublishTimestamp"),
        current_timestamp().as("speedIngestTimestamp")
      ).withColumn("timestamp", unix_timestamp($"speedObservation.timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

    (flowStream, speedStream)
  }

  /**
    * Joins the flow and speed streams
    *
    * @param parsedFlowStream  Dataframe with flow events
    * @param parsedSpeedStream Dataframe with speed events
    * @return Dataframe with joined events of both streams
    */
  def joinStage(parsedFlowStream: DataFrame, parsedSpeedStream: DataFrame): DataFrame = {
    val flowStreamWithWatermark = parsedFlowStream
      .withColumn("window", window(col("timestamp"),
        settings.general.publishIntervalMillis + " milliseconds",
        settings.general.publishIntervalMillis + " milliseconds"))
      .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")

    val speedStreamWithWatermark = parsedSpeedStream
      .withColumn("window", window(col("timestamp"),
        settings.general.publishIntervalMillis + " milliseconds",
        settings.general.publishIntervalMillis + " milliseconds"))
      .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")

    val latestTimestampUDF = udf((flowTimestamp: Timestamp, speedTimestamp: Timestamp) =>
      if (flowTimestamp.before(speedTimestamp)) speedTimestamp else flowTimestamp)
    val joinedSpeedAndFlowStreams = flowStreamWithWatermark
      .join(speedStreamWithWatermark, Seq("measurementId", "internalId", "timestamp", "window"))
      .select(
        $"measurementId",
        $"internalId".as("lanes"),
        $"timestamp",
        $"flowObservation.lat".as("latitude"),
        $"flowObservation.long".as("longitude"),
        $"flowObservation.flow".as("accumulatedFlow"),
        $"flowObservation.period".as("period"),
        $"flowObservation.accuracy".as("flowAccuracy"),
        $"speedObservation.speed".as("averageSpeed"),
        $"speedObservation.accuracy".as("speedAccuracy"),
        $"flowObservation.num_lanes".as("numLanes"),
        latestTimestampUDF($"flowPublishTimestamp", $"speedPublishTimestamp").as("publishTimestamp"),
        latestTimestampUDF($"flowIngestTimestamp", $"speedIngestTimestamp").as("ingestTimestamp")
      )

    joinedSpeedAndFlowStreams
  }
}