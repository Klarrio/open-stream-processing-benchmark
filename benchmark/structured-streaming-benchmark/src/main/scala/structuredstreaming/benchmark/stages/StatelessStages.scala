package structuredstreaming.benchmark.stages

import java.sql.Timestamp
import java.text.SimpleDateFormat

import common.benchmark.stages.StatelessStagesTemplate
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
class StatelessStages(sparkSession: SparkSession, settings: BenchmarkSettingsForStructuredStreaming)
  extends Serializable with StatelessStagesTemplate {

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
      .option("maxOffsetsPerTrigger", (settings.general.volume+1)*190*20) // read max 10 seconds of data
      .load()

    val speedStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", settings.general.kafkaBootstrapServers)
      .option("subscribe", settings.general.speedTopic)
      .option("startingOffsets", settings.general.kafkaAutoOffsetReset)
      .option("includeTimestamp", true)
      .option("maxOffsetsPerTrigger", (settings.general.volume+1)*190*20) // read max 10 seconds of data
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
      StructField("internalId", StringType, false), StructField("lat", DoubleType, true), StructField("long", DoubleType, true), StructField("flow", IntegerType, true),
      StructField("period", IntegerType, true), StructField("accuracy", IntegerType, true), StructField("timestamp", StringType, true),
      StructField("num_lanes", IntegerType, true)
    ))
    val speedSchema = StructType(Seq(
      StructField("internalId", StringType, false), StructField("lat", DoubleType, true), StructField("long", DoubleType, true), StructField("speed", DoubleType, true),
      StructField("accuracy", IntegerType, true), StructField("timestamp", StringType, true),
      StructField("num_lanes", IntegerType, true)
    ))

    val flowStream = rawFlowStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(
        col("key").as("measurementId"),
        from_json($"value", flowSchema).as("flowObservation"),
        col("timestamp").as("flowPublishTimestamp")
      ).withColumn("internalId", col("flowObservation.internalId"))
      .withColumn("timestamp", unix_timestamp($"flowObservation.timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

    val speedStream = rawSpeedStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(
        col("key").as("measurementId"),
        from_json($"value", speedSchema).as("speedObservation"),
        col("timestamp").as("speedPublishTimestamp")
      ).withColumn("internalId", col("speedObservation.internalId"))
      .withColumn("timestamp", unix_timestamp($"speedObservation.timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

    (flowStream, speedStream)
  }

  /**
   * Consumes from Kafka from flow topic
   *
   * @return DataFrames for speed and flow data
   */
  def ingestFlowStreamStage(): DataFrame = {
    val flowStream = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", settings.general.kafkaBootstrapServers)
      .option("subscribe", settings.general.flowTopic)
      .option("startingOffsets", settings.general.kafkaAutoOffsetReset)
      .option("includeTimestamp", true)
//      .option("maxOffsetsPerTrigger", settings.general.volume*190*10) // read max 10 seconds of data
      .load()

    flowStream
  }

  /**
   * Parses only flow events
   *
   * @return parsed Dataframes for speed and flow
   */
  def parsingFlowStreamStage(rawFlowStream: DataFrame): DataFrame = {

    val flowSchema = StructType(Seq(
      StructField("internalId", StringType, false), StructField("lat", DoubleType, true), StructField("long", DoubleType, true), StructField("flow", IntegerType, true),
      StructField("period", IntegerType, true), StructField("accuracy", IntegerType, true), StructField("timestamp", StringType, true),
      StructField("num_lanes", IntegerType, true)
    ))

    val flowStream = rawFlowStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(
        col("key").as("measurementId"),
        from_json($"value", flowSchema).as("flowObservation"),
        col("timestamp").as("flowPublishTimestamp")
      ).withColumn("internalId", col("flowObservation.internalId"))
      .withColumn("timestamp", unix_timestamp($"flowObservation.timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .select(
        $"measurementId",
        lit(1).as("laneCount"),
        $"timestamp",
        $"flowPublishTimestamp".as("publishTimestamp"),
        $"flowObservation.lat".as("latitude"),
        $"flowObservation.long".as("longitude"),
        $"flowObservation.flow".as("accumulatedFlow"),
        $"flowObservation.period".as("period"),
        $"flowObservation.accuracy".as("flowAccuracy"),
        $"flowObservation.num_lanes".as("numLanes")
      )

    flowStream
  }

}