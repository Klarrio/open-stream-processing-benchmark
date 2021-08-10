package structuredstreaming.benchmark

import java.sql.Timestamp

import common.config.LastStage._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import structuredstreaming.benchmark.stages.{StatefulStages, OutputUtils, StatelessStages}

/**
 * Starting point of Structured Streaming Traffic Analyzer
 * SparkContext
 * Analyzes speed and flow traffic data of NDW (National Data Warehouse of Traffic Information) of the Netherlands.
 * http://www.ndw.nu/en/
 *
 * Makes use of Apache Spark and Apache Kafka
 */

object StructuredStreamingTrafficAnalyzer {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Calls application skeleton with Spark configuration
   *
   * @param args application parameters
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    import BenchmarkSettingsForStructuredStreaming._
    val overrides = Seq(
      sparkConf.getOption("spark.MODE").keyedWith("environment.mode"),
      sparkConf.getOption("spark.KAFKA_BOOTSTRAP_SERVERS").keyedWith("kafka.bootstrap.servers"),
      sparkConf.getOption("spark.LAST_STAGE").keyedWith("general.last.stage"),
      sparkConf.getOption("spark.KAFKA_AUTO_OFFSET_RESET_STRATEGY").keyedWith("kafka.auto.offset.reset.strategy"),
      sparkConf.getOption("spark.METRICS_TOPIC").keyedWith("kafka.output.topic"),
      sparkConf.getOption("spark.FLOWTOPIC").keyedWith("kafka.flow.topic"),
      sparkConf.getOption("spark.SPEEDTOPIC").keyedWith("kafka.speed.topic"),
      sparkConf.getOption("spark.VOLUME").keyedWith("general.stream.source.volume"),
      sparkConf.getOption("spark.ACTIVE_HDFS_NAME_NODE").keyedWith("hdfs.active.name.node"),
      sparkConf.getOption("spark.sql.shuffle.partitions").keyedWith("spark.sql.shuffle.partitions"),
      sparkConf.getOption("spark.default.parallelism").keyedWith("spark.default.parallelism")
    ).flatten.toMap

    val settings = new BenchmarkSettingsForStructuredStreaming(overrides)

    run(settings)
  }

  /**
   * Executes general application skeleton
   *
   * In the configuration, you can specify till which stage you want the flow to be executed
   *
   * - Initializes Spark
   * - Parses and joins the flow and speed streams
   * - Aggregates the observations per measurement ID
   * - Computes the relative change
   * - Prints the [[common.benchmark.RelativeChangeObservation]]
   *
   * @param settings Spark configuration properties
   */
  def run(settings: BenchmarkSettingsForStructuredStreaming): Unit = {
    val sparkSession = initSpark(settings)
    import sparkSession.implicits._

    val statelessStages = new StatelessStages(sparkSession, settings)
    val statefulStages = new StatefulStages(sparkSession, settings)
    val outputUtils = new OutputUtils(sparkSession, settings)

    registerCorrectPartialFlowRun(settings, sparkSession, statelessStages, statefulStages, outputUtils)
  }

  def registerCorrectPartialFlowRun(settings: BenchmarkSettingsForStructuredStreaming, sparkSession: SparkSession,
    statelessStages: StatelessStages, statefulStages: StatefulStages, outputUtils: OutputUtils)
  : Unit = {
    import sparkSession.implicits._
    settings.general.lastStage match {
      case UNTIL_INGEST =>
        val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage()

        val outputSpeedStream = rawSpeedStream
          .withColumn("publishTimestamp", outputUtils.timeUDF($"timestamp")).drop("timestamp")
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .select($"key", to_json(struct($"publishTimestamp", $"jobProfile")).as("value"))

        val outputFlowStream = rawFlowStream
          .withColumn("publishTimestamp", outputUtils.timeUDF($"timestamp")).drop("timestamp")
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .select($"key", to_json(struct($"publishTimestamp", $"jobProfile")).as("value"))


        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(outputSpeedStream)
          outputUtils.printToConsole(outputFlowStream, awaitTermination = true)
        } else {
          outputUtils.writeToKafka(outputSpeedStream, queryNbr = 1)
          outputUtils.writeToKafka(outputFlowStream, queryNbr = 2, awaitTermination = !settings.general.shouldPrintOutput)
        }

      case UNTIL_PARSE =>
        val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage()
        val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
        val flowStreamWithTime = flowStream
          .withColumn("publishTimestamp", outputUtils.timeUDF($"flowPublishTimestamp")).drop("flowPublishTimestamp")
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
        val outputFlowStream = flowStreamWithTime
          .select(col("measurementId").as("key"), to_json(struct(flowStreamWithTime.columns.map(col(_)): _*)).as("value"))

        val speedStreamWithTime = speedStream
          .withColumn("publishTimestamp", outputUtils.timeUDF($"speedPublishTimestamp")).drop("speedPublishTimestamp")
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
        val outputSpeedStream = speedStreamWithTime
          .select(col("measurementId").as("key"), to_json(struct(speedStreamWithTime.columns.map(col(_)): _*)).as("value"))


        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(flowStream)
          outputUtils.printToConsole(speedStream, awaitTermination = true)
        } else {
          outputUtils.writeToKafka(outputFlowStream, queryNbr = 1)
          outputUtils.writeToKafka(outputSpeedStream, queryNbr = 2, awaitTermination = !settings.general.shouldPrintOutput)
        }

      case UNTIL_JOIN =>
        val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage()
        val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
        val joinedSpeedAndFlowStreams = statefulStages.joinStage(flowStream, speedStream)
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .withColumn("publishTimestamp", outputUtils.timeUDF($"publishTimestamp"))
        val outputJoinedStream = joinedSpeedAndFlowStreams
          .select(col("measurementId").as("key"), to_json(struct(joinedSpeedAndFlowStreams.columns.map(col(_)): _*)).as("value"))

        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(outputJoinedStream, awaitTermination = true)
        } else {
          outputUtils.writeToKafka(outputJoinedStream, awaitTermination = !settings.general.shouldPrintOutput)
        }

      case UNTIL_TUMBLING_WINDOW =>
        val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage()
        val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
        val joinedSpeedAndFlowStreams = statefulStages.joinStage(flowStream, speedStream)
        val aggregatedStream = statefulStages.aggregationAfterJoinStage(joinedSpeedAndFlowStreams)
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .withColumn("publishTimestamp", outputUtils.timeUDF($"publishTimestamp"))

        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(aggregatedStream, awaitTermination = true)
        } else {
          val outputAggregatedStream = aggregatedStream
            .select(col("measurementId").as("key"), to_json(struct(aggregatedStream.columns.map(col(_)): _*)).as("value"))
          outputUtils.writeToKafka(outputAggregatedStream, awaitTermination = !settings.general.shouldPrintOutput)
        }

      case UNTIL_LOWLEVEL_TUMBLING_WINDOW =>
        val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage()
        val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
        val joinedSpeedAndFlowStreams = statefulStages.joinStage(flowStream, speedStream)
        val aggregatedStream = statefulStages.lowLevelAggregationAfterJoinStage(joinedSpeedAndFlowStreams)
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .withColumn("publishTimestamp", outputUtils.timeUDF($"publishTimestamp"))

        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(aggregatedStream, awaitTermination = true)
        } else {
          val outputAggregatedStream = aggregatedStream
            .select(col("measurementId").as("key"), to_json(struct(aggregatedStream.columns.map(col(_)): _*)).as("value"))
          outputUtils.writeToKafka(outputAggregatedStream, awaitTermination = !settings.general.shouldPrintOutput)
        }

      case UNTIL_LOWLEVEL_SLIDING_WINDOW =>
        val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage()
        val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
        val joinedSpeedAndFlowStreams = statefulStages.joinStage(flowStream, speedStream)
        val relativeChangeStream = statefulStages.lowLevelAggregationAndSlidingWindowStage(joinedSpeedAndFlowStreams)
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .withColumn("publishTimestamp", outputUtils.timeUDF($"publishTimestamp"))

        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(relativeChangeStream, awaitTermination = true)
        } else {
          val outputAggregatedStream = relativeChangeStream
            .select(col("measurementId").as("key"), to_json(struct(relativeChangeStream.columns.map(col(_)): _*)).as("value"))
          outputUtils.writeToKafka(outputAggregatedStream, awaitTermination = !settings.general.shouldPrintOutput)
        }


      case REDUCE_WINDOW_WITHOUT_JOIN =>
        val rawFlowStream = statelessStages.ingestFlowStreamStage()
        val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
        val aggregatedFlowStream = statefulStages.reduceWindowAfterParsingStage(flowStream)
        val aggregatedFlowStreamWithTime = aggregatedFlowStream
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .withColumn("publishTimestamp", outputUtils.timeUDF($"publishTimestamp"))

        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(aggregatedFlowStreamWithTime, awaitTermination = true)
        } else {
          val outputFlowAggregatedStream = aggregatedFlowStreamWithTime
            .select(col("measurementId").as("key"), to_json(struct(aggregatedFlowStreamWithTime.columns.map(col(_)): _*)).as("value"))
          outputUtils.writeToKafka(outputFlowAggregatedStream, queryNbr = 1, awaitTermination = !settings.general.shouldPrintOutput)
        }


      case NON_INCREMENTAL_WINDOW_WITHOUT_JOIN =>
        val rawFlowStream = statelessStages.ingestFlowStreamStage()
        val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
        val aggregatedFlowStream = statefulStages.nonIncrementalWindowAfterParsingStage(flowStream)
        val aggregatedFlowStreamWithTime = aggregatedFlowStream
          .withColumn("jobProfile", lit(settings.specific.jobProfileKey))
          .withColumn("publishTimestamp", outputUtils.timeUDF($"publishTimestamp"))

        if (settings.general.shouldPrintOutput) {
          outputUtils.printToConsole(aggregatedFlowStreamWithTime, awaitTermination = true)
        } else {
          val outputFlowAggregatedStream = aggregatedFlowStreamWithTime
            .select(col("measurementId").as("key"), to_json(struct(aggregatedFlowStreamWithTime.columns.map(col(_)): _*)).as("value"))
          outputUtils.writeToKafka(outputFlowAggregatedStream, queryNbr = 1, awaitTermination = !settings.general.shouldPrintOutput)
        }
    }
  }

  /**
   * Initializes Spark
   *
   * @param settings Spark configuration properties
   * @return Spark session and streaming context
   */
  def initSpark(settings: BenchmarkSettingsForStructuredStreaming): SparkSession = {
    val sparkSession = SparkSession.builder()
      .master(settings.specific.sparkMaster)
      .appName("structured-streaming-benchmark")
//      .config("spark.sql.streaming.multipleWatermarkPolicy", "min")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(getKryoConfig)
      .config("spark.sql.streaming.checkpointLocation", settings.specific.checkpointDir)
      .config("spark.locality.wait", settings.specific.localityWait)
      .config("spark.streaming.receiver.writeAheadLog.enable", settings.specific.writeAheadLogEnabled)
      .config("spark.streaming.blockInterval", settings.specific.blockInterval)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.sql.streaming.minBatchesToRetain", 2)
      .getOrCreate()

    sparkSession
  }

  private def getKryoConfig = {
    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
        classOf[org.apache.spark.sql.execution.streaming.sources.PackedRowCommitMessage],
        classOf[Array[org.apache.spark.sql.Row]],
        classOf[org.apache.spark.sql.Row],
        classOf[Array[Object]],
        classOf[scala.collection.mutable.WrappedArray$ofRef],
        classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
        classOf[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema],
        classOf[Array[org.apache.spark.sql.types.StructField]],
        classOf[org.apache.spark.sql.types.StructType],
        classOf[org.apache.spark.sql.types.StructField],
        classOf[org.apache.spark.sql.types.StringType$],
        classOf[org.apache.spark.sql.types.IntegerType$],
        classOf[org.apache.spark.sql.types.DoubleType$],
        classOf[org.apache.spark.sql.types.TimestampType$],
        classOf[org.apache.spark.sql.types.LongType$],
        classOf[org.apache.spark.sql.types.BinaryType$],
        classOf[org.apache.spark.sql.types.ArrayType],
        classOf[org.apache.spark.sql.types.Metadata],
        classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow]
      )
    )
  }
}