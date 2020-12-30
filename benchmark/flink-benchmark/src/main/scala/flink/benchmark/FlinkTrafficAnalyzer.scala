
/**
  * Starting point of Flink Traffic Analyzer
  *
  * Analyzes speed and flow traffic data of NDW (National Data Warehouse of Traffic Information) of the Netherlands.
  * http://www.ndw.nu/en/
  *
  * Makes use of Apache Flink and Apache Kafka
  *
  */

package flink.benchmark

import java.util.Properties

import common.benchmark.output.JsonPrinter
import common.config.LastStage
import common.config.LastStage._
import flink.benchmark.stages.{OutputMessageSerializer, StatefulStages, StatelessStages}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic

object FlinkTrafficAnalyzer {

  /**
    * Calls application skeleton with Flink configuration
    *
    * @param args application parameters
    */
  def main(args: Array[String]): Unit = {
    val settings = new BenchmarkSettingsForFlink
    run(settings)
  }

  /**
    * Executes general application skeleton
    *
    * - Initializes Flink
    * - Initializes Kafka
    * - Parses and joins the flow and speed streams
    * - Aggregates the observations per measurement ID
    * - Computes the relative change
    * - Prints the [[common.benchmark.RelativeChangeObservation]]
    *
    * @param settings Flink configuration properties
    */
  def run(settings: BenchmarkSettingsForFlink): Unit = {
    val executionEnvironment = initFlink(settings)

    val kafkaProperties = initKafka(settings)
    val statelessStages = new StatelessStages(settings, kafkaProperties)
    val statefulStages = new StatefulStages(settings)

    val processingSemantic = if(settings.specific.exactlyOnce) Semantic.EXACTLY_ONCE
    else Semantic.AT_LEAST_ONCE
    val kafkaProducer = new FlinkKafkaProducer[(String, String)](
      settings.general.outputTopic,
      new OutputMessageSerializer(settings),
      kafkaProperties,
      processingSemantic)

    registerCorrectPartialFlowForRun(settings, executionEnvironment, kafkaProducer, statelessStages, statefulStages)

    executionEnvironment.execute("Flink Traffic Analyzer")
  }

  def registerCorrectPartialFlowForRun(settings: BenchmarkSettingsForFlink,
    executionEnvironment: StreamExecutionEnvironment, kafkaProducer: FlinkKafkaProducer[(String, String)],
    statelessStages: StatelessStages, statefulStages: StatefulStages)
  : Unit = settings.general.lastStage match {
    case UNTIL_INGEST =>
      val rawStream = statelessStages.ingestStage(executionEnvironment)
      rawStream.map(r => JsonPrinter.jsonFor(r, settings.specific.jobProfileKey)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        rawStream.print()
      }

    case UNTIL_PARSE =>
      val rawStream = statelessStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawStream)
      flowStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      speedStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        flowStream.print()
        speedStream.print()
      }

    case UNTIL_JOIN =>
      val rawStream = statelessStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawStream)
      val joinedSpeedAndFlowStreams = statefulStages.intervalJoinStage(flowStream, speedStream)
      joinedSpeedAndFlowStreams.map { r => JsonPrinter.jsonFor(r) }.addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        joinedSpeedAndFlowStreams.print()
      }

    case UNTIL_TUMBLING_WINDOW =>
      val rawStream = statelessStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawStream)
      val joinedSpeedAndFlowStreams = statefulStages.intervalJoinStage(flowStream, speedStream)
      val aggregateStream = statefulStages.aggregationAfterJoinStage(joinedSpeedAndFlowStreams)
      aggregateStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        aggregateStream.print()
      }

    case UNTIL_SLIDING_WINDOW =>
      val rawStream = statelessStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawStream)
      val joinedSpeedAndFlowStreams = statefulStages.intervalJoinStage(flowStream, speedStream)
      val aggregateStream = statefulStages.aggregationAfterJoinStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream = statefulStages.slidingWindowAfterAggregationStage(aggregateStream)
      relativeChangeStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        relativeChangeStream.print()
      }

    case UNTIL_LOWLEVEL_TUMBLING_WINDOW =>
      val rawStream = statelessStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawStream)
      val joinedSpeedAndFlowStreams = statefulStages.intervalJoinStage(flowStream, speedStream)
      val aggregateStream = statefulStages.lowLevelAggregationAfterJoinStage(joinedSpeedAndFlowStreams)
      aggregateStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        aggregateStream.print()
      }

    case UNTIL_LOWLEVEL_SLIDING_WINDOW =>
      val rawStream = statelessStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawStream)
      val joinedSpeedAndFlowStreams = statefulStages.intervalJoinStage(flowStream, speedStream)
      val aggregateStream = statefulStages.lowLevelAggregationAfterJoinStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream = statefulStages.lowLevelSlidingWindowAfterAggregationStage(aggregateStream)
      relativeChangeStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        relativeChangeStream.print()
      }

    case LastStage.REDUCE_WINDOW_WITHOUT_JOIN =>
      val rawFlowStream = statelessStages.ingestFlowStreamStage(executionEnvironment)
      val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
      val aggregatedFlowStream = statefulStages.reduceWindowAfterParsingStage(flowStream)
      aggregatedFlowStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        aggregatedFlowStream.print()
      }

    case LastStage.NON_INCREMENTAL_WINDOW_WITHOUT_JOIN =>
      val rawFlowStream = statelessStages.ingestFlowStreamStage(executionEnvironment)
      val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
      val aggregatedFlowStream = statefulStages.nonIncrementalWindowAfterParsingStage(flowStream)
      aggregatedFlowStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        aggregatedFlowStream.print()
      }
  }

  /**
    * Initializes Flink
    *
    * @param settings Flink configuration properties
    * @return stream execution environment of Flink
    */
  def initFlink(settings: BenchmarkSettingsForFlink): StreamExecutionEnvironment = {
    val executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    executionEnvironment.setParallelism(settings.specific.partitions)
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // The interval at which the getCurrentWatermark method is called from the WatermarkAssigners
    executionEnvironment.getConfig.setAutoWatermarkInterval(settings.specific.autoWatermarkInterval)
    executionEnvironment.setBufferTimeout(settings.specific.bufferTimeout)
    executionEnvironment.enableCheckpointing(settings.specific.checkpointInterval)
    executionEnvironment.getConfig.enableObjectReuse()

    val stateBackend: StateBackend = new FsStateBackend(settings.specific.checkpointDir, true)
//    val stateBackend: StateBackend = new RocksDBStateBackend(settings.specific.checkpointDir, true)
    executionEnvironment.setStateBackend(stateBackend)
  }

  /**
    * Initializes Kafka
    *
    * @param settings Flink configuration properties
    * @return [[Properties]] for Kafka
    */
  def initKafka(settings: BenchmarkSettingsForFlink): Properties = {
    val timeToString = "FLINK/" + System.currentTimeMillis()
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", settings.general.kafkaBootstrapServers)
    kafkaProperties.setProperty("group.id", timeToString)
    if(settings.general.local) kafkaProperties.setProperty("transaction.timeout.ms", "900000")
    kafkaProperties.setProperty("enable.idempotence", "true")
    kafkaProperties
  }
}