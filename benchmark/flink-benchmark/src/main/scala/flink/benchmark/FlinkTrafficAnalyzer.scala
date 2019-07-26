
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
import flink.benchmark.stages.{AnalyticsStages, InitialStages, OutputMessageSerializer}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import common.config.LastStage._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.api.scala._

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
    val initialStages = new InitialStages(settings, kafkaProperties)
    val analyticsStages = new AnalyticsStages(settings, kafkaProperties)
    val kafkaProducer = new FlinkKafkaProducer010[String](settings.general.outputTopic, new OutputMessageSerializer(settings), kafkaProperties)

    registerCorrectPartialFlowForRun(settings, executionEnvironment, kafkaProducer, initialStages, analyticsStages)

    executionEnvironment.execute("Flink Traffic Analyzer")
  }

  def registerCorrectPartialFlowForRun(settings: BenchmarkSettingsForFlink,
    executionEnvironment: StreamExecutionEnvironment, kafkaProducer: FlinkKafkaProducer010[String],
    initialStages: InitialStages, analyticsStages: AnalyticsStages)
  : Unit = settings.general.lastStage match {
    case UNTIL_INGEST =>
      val rawStreams = initialStages.ingestStage(executionEnvironment)
      rawStreams.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        rawStreams.print()
      }

    case UNTIL_PARSE =>
      val rawStreams = initialStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = initialStages.parsingStage(rawStreams)
      flowStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      speedStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        flowStream.print()
        speedStream.print()
      }

    case UNTIL_JOIN =>
      val rawStreams = initialStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = initialStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(flowStream, speedStream)
      joinedSpeedAndFlowStreams.map { r => JsonPrinter.jsonFor(r) }.addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        joinedSpeedAndFlowStreams.print()
      }

    case UNTIL_TUMBLING_WINDOW =>
      val rawStreams = initialStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = initialStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(flowStream, speedStream)
      val aggregateStream = analyticsStages.aggregationStage(joinedSpeedAndFlowStreams)
      aggregateStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        aggregateStream.print()
      }

    case UNTIL_SLIDING_WINDOW =>
      val rawStreams = initialStages.ingestStage(executionEnvironment)
      val (flowStream, speedStream) = initialStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(flowStream, speedStream)
      val aggregateStream = analyticsStages.aggregationStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream = analyticsStages.relativeChangeStage(aggregateStream)
      relativeChangeStream.map(r => JsonPrinter.jsonFor(r)).addSink(kafkaProducer)
      if (settings.general.shouldPrintOutput) {
        relativeChangeStream.print()
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
    executionEnvironment.setParallelism(settings.general.partitions)
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // The interval at which the getCurrentWatermark method is called from the WatermarkAssigners
    executionEnvironment.getConfig.setAutoWatermarkInterval(settings.specific.autoWatermarkInterval)
    executionEnvironment.setBufferTimeout(settings.specific.bufferTimeout)
    executionEnvironment.enableCheckpointing(settings.specific.checkpointInterval)

    executionEnvironment.getConfig.enableObjectReuse()


    val stateBackend: FsStateBackend = new FsStateBackend(settings.specific.checkpointDir, true)
    executionEnvironment.setStateBackend(stateBackend)
  }

  /**
    * Initializes Kafka
    *
    * @param settings Flink configuration properties
    * @return [[Properties]] for Kafka
    */
  def initKafka(settings: BenchmarkSettingsForFlink): Properties = {
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", settings.general.kafkaBootstrapServers)
    kafkaProperties
  }
}