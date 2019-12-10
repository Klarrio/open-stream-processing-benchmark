/**
  * Starting point of Kafka Traffic Analyzer
  *
  * Analyzes speed and flow traffic data of NDW (National Data Warehouse of Traffic Information) of the Netherlands.
  * http://www.ndw.nu/en/
  *
  * Makes use of Apache Kafka and Kafka-Streams
  */

package kafka.benchmark

import java.sql.Timestamp
import java.util.Properties

import common.benchmark.output.JsonPrinter
import common.benchmark.{AggregatableObservation, FlowObservation, RelativeChangeObservation, SpeedObservation}
import common.config.JobExecutionMode.{CONSTANT_RATE, LATENCY_CONSTANT_RATE, SINGLE_BURST}
import common.config.LastStage._
import kafka.benchmark.stages._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.{ProcessorContext, TimestampExtractor}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.util.Try

object KafkaTrafficAnalyzer {
  /**
    * Calls application skeleton with Spark configuration
    *
    * @param args application parameters
    */
  def main(args: Array[String]): Unit = {
    import BenchmarkSettingsForKafkaStreams._
    def getEnv(name: String): Option[String] = Try(sys.env(name).toString).toOption

    val overrides = Seq(
      getEnv("MODE").keyedWith("environment.mode"),
      getEnv("KAFKA_BOOTSTRAP_SERVERS").keyedWith("kafka.bootstrap.servers"),
      getEnv("LAST_STAGE").keyedWith("general.last.stage"),
      getEnv("KAFKA_AUTO_OFFSET_RESET_STRATEGY").keyedWith("kafka.auto.offset.reset.strategy"),
      getEnv("METRICS_TOPIC").keyedWith("kafka.output.topic"),
      getEnv("FLOWTOPIC").keyedWith("kafka.flow.topic"),
      getEnv("SPEEDTOPIC").keyedWith("kafka.speed.topic"),
      getEnv("VOLUME").keyedWith("general.stream.source.volume"),
      getEnv("BUFFER_TIMEOUT").keyedWith("general.buffer.timeout")
    ).flatten.toMap

    val settings = new BenchmarkSettingsForKafkaStreams(overrides)
    run(settings)
  }


  /**
    * Executes general application skeleton
    *
    * In the configuration, you can specify till which stage you want the flow to be executed
    *
    * - Initializes Kafka
    * - Parses and joins the flow and speed streams
    * - Aggregates the observations per measurement ID
    * - Computes the relative change
    * - Prints the [[common.benchmark.RelativeChangeObservation]]
    *
    * @param settings Kafka-streams configuration properties
    */
  def run(settings: BenchmarkSettingsForKafkaStreams): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val props = initKafka(settings)

    val builder = new StreamsBuilder()

    val initialStages = new InitialStages(settings)
    val analyticsStages = new AnalyticsStages(settings)
    val observationFormatter = new ObservationFormatter(settings.specific.jobProfileKey)

    if (settings.specific.useCustomTumblingWindow) {
      val persistentKeyValueStore: StoreBuilder[KeyValueStore[String, AggregatableObservation]] = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("lane-aggregator-state-store"),
          CustomObjectSerdes.StringSerde,
          CustomObjectSerdes.AggregatableObservationSerde
        )
        .withCachingEnabled()
      builder.addStateStore(persistentKeyValueStore)
    }

    if (settings.specific.useCustomSlidingWindow) {
      val persistentKeyValueStore: StoreBuilder[KeyValueStore[String, List[AggregatableObservation]]] = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("relative-change-state-store"),
          CustomObjectSerdes.StringSerde,
          CustomObjectSerdes.AggregatedObservationListSerde
        )
        .withCachingEnabled()
      builder.addStateStore(persistentKeyValueStore)
    }

    registerCorrectPartialFlowForRun(settings, builder, observationFormatter, initialStages, analyticsStages)

    val topology = builder.build()
    logger.info("Topology description {}", topology.describe)
    val streams = new KafkaStreams(topology, props)

    streams.start()
    Thread.sleep(10000000)
    streams.close()
  }

  class ObservationFormatter(jobProfileKey: String) extends Serializable {
    def pub(obs: (String, String, Long)): (String, String) = (jobProfileKey, JsonPrinter.jsonFor(obs))

    def pub(obs: SpeedObservation): (String, String) = (jobProfileKey, JsonPrinter.jsonFor(obs))

    def pub(obs: FlowObservation): (String, String) = (jobProfileKey, JsonPrinter.jsonFor(obs))

    def pub(obs: AggregatableObservation): (String, String) = (jobProfileKey, JsonPrinter.jsonFor(obs))

    def pub(obs: RelativeChangeObservation): (String, String) = (jobProfileKey, JsonPrinter.jsonFor(obs))
  }

  def registerCorrectPartialFlowForRun(settings: BenchmarkSettingsForKafkaStreams,
    builder: StreamsBuilder, observationFormatter: ObservationFormatter,
    initialStages: InitialStages, analyticsStages: AnalyticsStages)
  : Unit = settings.general.lastStage match {
    case UNTIL_INGEST =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(builder)

      rawFlowStream.transform(new TransformerSupplier[String, String, KeyValue[String, String]] {
        override def get(): Transformer[String, String, KeyValue[String, String]] = {
          new Transformer[String, String, KeyValue[String, String]] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext): Unit = {
              processorContext = context
            }

            override def transform(key: String, value: String): KeyValue[String, String] = {
              val (parsedKey, parsedValue) = observationFormatter.pub(processorContext.topic(), value, processorContext.timestamp())
              new KeyValue[String, String](parsedKey, parsedValue)
            }

            override def close(): Unit = {}
          }
        }
      }).to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      rawSpeedStream.transform(new TransformerSupplier[String, String, KeyValue[String, String]] {
        override def get(): Transformer[String, String, KeyValue[String, String]] = {
          new Transformer[String, String, KeyValue[String, String]] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext): Unit = {
              processorContext = context
            }

            override def transform(key: String, value: String): KeyValue[String, String] = {
              val (parsedKey, parsedValue) = observationFormatter.pub(processorContext.topic(), value, processorContext.timestamp())
              new KeyValue[String, String](parsedKey, parsedValue)
            }

            override def close(): Unit = {}
          }
        }
      }).to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))
      if (settings.general.shouldPrintOutput) {
        rawFlowStream.print(Printed.toSysOut[String, String]())
        rawSpeedStream.print(Printed.toSysOut[String, String]())
      }
    case UNTIL_PARSE =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)

      parsedFlowStream.map { (key: String, obs: FlowObservation) => observationFormatter.pub(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))
      parsedSpeedStream.map { (key: String, obs: SpeedObservation) => observationFormatter.pub(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        parsedFlowStream.print(Printed.toSysOut[String, FlowObservation]())
        parsedSpeedStream.print(Printed.toSysOut[String, SpeedObservation]())
      }

    case UNTIL_JOIN =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(parsedFlowStream, parsedSpeedStream)

      joinedSpeedAndFlowStreams.map { (key: String, obs: AggregatableObservation) => observationFormatter.pub(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        joinedSpeedAndFlowStreams.print(Printed.toSysOut())
      }

    case UNTIL_TUMBLING_WINDOW =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(parsedFlowStream, parsedSpeedStream)
      val aggregateStream = analyticsStages.aggregationStage(joinedSpeedAndFlowStreams)

      aggregateStream.map { (_: String, obs: AggregatableObservation) => observationFormatter.pub(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        aggregateStream
          .print(Printed.toSysOut())
      }

    case UNTIL_SLIDING_WINDOW =>
      val (rawFlowStream, rawSpeedStream) = initialStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = initialStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams = initialStages.joinStage(parsedFlowStream, parsedSpeedStream)
      val aggregateStream = analyticsStages.aggregationStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream: kstream.KStream[String, RelativeChangeObservation] = analyticsStages.relativeChangeStage(aggregateStream)

      relativeChangeStream.map { (key: String, obs: RelativeChangeObservation) => observationFormatter.pub(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        val toPrintStream: kstream.KStream[String, String] = relativeChangeStream
          .map { (key: String, obs: RelativeChangeObservation) => (new Timestamp(obs.publishTimestamp).toString, obs.toJsonString()) }
        toPrintStream.print(Printed.toSysOut[String, String]())
      }
  }

  /**
    * Initializes Kafka
    *
    * @param settings Kafka-streams configuration properties
    * @return [[Properties]] containing Kafka configuration properties
    */
  def initKafka(settings: BenchmarkSettingsForKafkaStreams): Properties = {
    val streamsConfiguration: Properties = new Properties()
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, settings.specific.applicationId)
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, settings.general.kafkaBootstrapServers)
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "./kafka-logs/")
    streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, settings.specific.numStreamsThreads.toString)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, settings.specific.commitInterval.toString)
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, settings.specific.cacheMaxBytesBuffering.toString)
    streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, settings.specific.maxTaskIdleMillis)

    // Producer Config
    streamsConfiguration.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, settings.specific.compressionTypeConfig.toString)
    streamsConfiguration.put(ProducerConfig.BATCH_SIZE_CONFIG, settings.specific.batchSize.toString)
    streamsConfiguration.put(ProducerConfig.LINGER_MS_CONFIG, settings.specific.lingerMs.toString)

    // Consumer Config
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, settings.general.kafkaAutoOffsetReset)

    // For monitoring of memory
    streamsConfiguration.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, classOf[JmxReporter].getName)
    streamsConfiguration.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "1000")

    streamsConfiguration
  }
}

