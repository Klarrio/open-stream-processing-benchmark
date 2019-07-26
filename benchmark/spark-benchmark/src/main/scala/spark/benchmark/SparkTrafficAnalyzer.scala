/**
 * Starting point of Spark Traffic Analyzer
 *
 * Analyzes speed and flow traffic data of NDW (National Data Warehouse of Traffic Information) of the Netherlands.
 * http://www.ndw.nu/en/
 *
 * Makes use of Apache Spark and Apache Kafka
 */

package spark.benchmark

import java.io.Serializable

import common.benchmark._
import common.config.LastStage
import common.config.LastStage._
import common.benchmark.output.{JsonPrinter, KafkaSinkForSparkAndStorm}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import spark.benchmark.stages.{AnalyticsStages, InitialStages}

object SparkTrafficAnalyzer {

  /**
   * Calls application skeleton with Spark configuration
   *
   * @param args application parameters
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    import BenchmarkSettingsForSpark._
    val overrides = Seq(
      sparkConf.getOption("spark.MODE").keyedWith("environment.mode"),
      sparkConf.getOption("spark.KAFKA_BOOTSTRAP_SERVERS").keyedWith("kafka.bootstrap.servers"),
      sparkConf.getOption("spark.LAST_STAGE").keyedWith("general.last.stage"),
      sparkConf.getOption("spark.KAFKA_AUTO_OFFSET_RESET_STRATEGY").keyedWith("kafka.auto.offset.reset.strategy"),
      sparkConf.getOption("spark.METRICS_TOPIC").keyedWith("kafka.output.topic"),
      sparkConf.getOption("spark.FLOWTOPIC").keyedWith("kafka.flow.topic"),
      sparkConf.getOption("spark.SPEEDTOPIC").keyedWith("kafka.speed.topic"),
      sparkConf.getOption("spark.VOLUME").keyedWith("general.stream.source.volume"),
      sparkConf.getOption("spark.ACTIVE_HDFS_NAME_NODE").keyedWith("hdfs.active.name.node")
    ).flatten.toMap

    val settings = new BenchmarkSettingsForSpark(overrides)
    run(settings)
  }



  /**
   * Executes general application skeleton
   *
   * In the configuration, you can specify till which stage you want the flow to be executed
   *
   * - Initializes Spark
   * - Initializes Kafka
   * - Parses and joins the flow and speed streams
   * - Aggregates the observations per measurement ID
   * - Computes the relative change
   * - Prints the [[RelativeChangeObservation]]
   *
   * @param settings Spark configuration properties
   */
  def run(settings: BenchmarkSettingsForSpark): Unit = {
    val (sparkSession, ssc) = initSpark(settings)

    val kafkaParams = initKafka(settings)

    val kafkaSink: Broadcast[KafkaSinkForSparkAndStorm] = sparkSession.sparkContext.broadcast(KafkaSinkForSparkAndStorm(settings.general.kafkaBootstrapServers, settings.general.outputTopic, settings.specific.jobProfileKey))

    val parseAndJoinUtils = new InitialStages(settings, kafkaParams)
    val aggregationAndWindowUtils = new AnalyticsStages(settings)
    val obsPublisher = new ObservationPublisher(kafkaSink)
    
    registerCorrectPartialFlowForRun(settings, ssc, obsPublisher, parseAndJoinUtils, aggregationAndWindowUtils)

    ssc.checkpoint(settings.specific.checkpointDir)
    ssc.start()
    ssc.awaitTerminationOrTimeout(60000)
  }


  class ObservationPublisher(kafkaSink: Broadcast[KafkaSinkForSparkAndStorm]) extends Serializable {
    def pub(rawInputRecord: (String, String, String, Long)) = kafkaSink.value.send(JsonPrinter.jsonFor(rawInputRecord ))
    def pub(obs: FlowObservation) = kafkaSink.value.send(JsonPrinter.jsonFor(obs))
    def pub(obs: SpeedObservation) = kafkaSink.value.send(JsonPrinter.jsonFor(obs))
    def pub(obs: AggregatableObservation) = kafkaSink.value.send(JsonPrinter.jsonFor(obs))
    def pub(obs: RelativeChangeObservation) = kafkaSink.value.send(JsonPrinter.jsonFor(obs))
  }

  def registerCorrectPartialFlowForRun(settings: BenchmarkSettingsForSpark,
    ssc: StreamingContext, toKafkaPublisher: ObservationPublisher,
    initialStages: InitialStages, aggregationAndWindowUtils: AnalyticsStages)
    : Unit = settings.general.lastStage match {

    case UNTIL_INGEST =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(ssc)
      rawFlowStream.foreachRDD { rdd => rdd.foreach { r => toKafkaPublisher.pub(r) } }
      rawSpeedStream.foreachRDD { rdd => rdd.foreach { r => toKafkaPublisher.pub(r) } }
      if (settings.general.shouldPrintOutput) {
        rawFlowStream.print()
        rawSpeedStream.print()
      }

    case UNTIL_PARSE =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(ssc)
      val (flowStream, speedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)

      flowStream.foreachRDD { rdd => rdd.foreach { case (key: String, obs: FlowObservation) =>
        toKafkaPublisher.pub(obs) } }
      speedStream.foreachRDD { rdd => rdd.foreach { case (key: String, obs: SpeedObservation) =>
        toKafkaPublisher.pub(obs) } }
      if (settings.general.shouldPrintOutput) {
        flowStream.print()
        speedStream.print()
      }

    case UNTIL_JOIN =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(ssc)
      val (flowStream, speedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(flowStream, speedStream)

      joinedSpeedAndFlowStreams.foreachRDD { rdd => rdd.foreach { case (key: String, (flow: FlowObservation, speed: SpeedObservation)) =>
            val obs = new AggregatableObservation(flow, speed)
            toKafkaPublisher.pub(obs) } }
      if (settings.general.shouldPrintOutput) {
        joinedSpeedAndFlowStreams.print()
      }

    case UNTIL_TUMBLING_WINDOW =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(ssc)
      val (flowStream, speedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams: DStream[(String, (FlowObservation, SpeedObservation))] = initialStages.joinStage(flowStream, speedStream)
      val aggregateStream: DStream[AggregatableObservation] = aggregationAndWindowUtils.aggregationStage(joinedSpeedAndFlowStreams)

      aggregateStream.foreachRDD { rdd => rdd.foreach { obs =>
        toKafkaPublisher.pub(obs) } }

      if (settings.general.shouldPrintOutput) {
        aggregateStream.print()
      }

    case UNTIL_SLIDING_WINDOW =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(ssc)
      val (flowStream, speedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(flowStream, speedStream)
      val aggregateStream = aggregationAndWindowUtils.aggregationStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream = aggregationAndWindowUtils.relativeChangesStage(aggregateStream)

      relativeChangeStream.foreachRDD { rdd => rdd.foreach { obs =>
        toKafkaPublisher.pub(obs) } }

      if (settings.general.shouldPrintOutput) relativeChangeStream.print()

  }

  /**
   * Initializes Spark
   *
   * @param sparkConfig Spark configuration properties
   * @return Spark session and streaming context
   */
  def initSpark(settings: BenchmarkSettingsForSpark): (SparkSession, StreamingContext) = {
    val sparkSession = SparkSession.builder()
      .master(settings.specific.sparkMaster)
      .appName("spark-streaming-benchmark" + System.currentTimeMillis())
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "true")
      .config(getKryoConfig)
      .config("spark.default.parallelism", settings.specific.parallelism)
      .config("spark.sql.shuffle.partitions", settings.specific.sqlShufflePartitions)
      .config("spark.sql.streaming.minBatchesToRetain", settings.specific.sqlMinBatchesToRetain)
      .config("spark.streaming.backpressure.enabled", settings.specific.backpressureEnabled)
      .config("spark.locality.wait", settings.specific.localityWait)
      .config("spark.streaming.blockInterval", settings.specific.blockInterval)
      .getOrCreate()


    val ssc = new StreamingContext(sparkSession.sparkContext, Milliseconds(settings.specific.batchInterval))
    (sparkSession, ssc)
  }

  /**
   * Initializes Kafka
   *
   * @param sparkConfig Spark configuration properties
   * @return [[Map]] containing Kafka configuration properties
   */
  def initKafka(settings: BenchmarkSettingsForSpark): Map[String, Object] = {
    val timeToString = "SPARK/" + System.currentTimeMillis()

    Map[String, Object](
      "bootstrap.servers" -> settings.general.kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> timeToString,
      "auto.offset.reset" -> settings.general.kafkaAutoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

  private def getKryoConfig = {
    val conf = new SparkConf()
    conf.registerKryoClasses(
      Array(
        classOf[SpeedObservation],
        classOf[FlowObservation],
        classOf[Array[common.benchmark.AggregatableObservation]],
        classOf[AggregatableObservation],
        classOf[RelativeChangeObservation],
        classOf[KafkaSinkForSparkAndStorm],
        classOf[common.benchmark.output.KafkaSinkForSparkAndStorm$$anonfun$1],
        classOf[Array[common.benchmark.RelativeChangeObservation]],
        classOf[scala.collection.mutable.WrappedArray$ofRef]
      )
    )
  }
}
