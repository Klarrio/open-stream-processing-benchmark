/**
 * Starting point of Kafka Traffic Analyzer
 *
 * Analyzes speed and flow traffic data of NDW (National Data Warehouse of Traffic Information) of the Netherlands.
 * http://www.ndw.nu/en/
 *
 * Makes use of Apache Kafka and Kafka-Streams
 */

package kafka.benchmark

import java.util
import java.util.Properties

import common.benchmark.output.JsonPrinter
import common.benchmark.{AggregatableFlowObservation, AggregatableObservation, FlowObservation, RelativeChangeObservation, SpeedObservation}
import common.config.LastStage._
import kafka.benchmark.stages._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.metrics.JmxReporter
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.rocksdb.{LRUCache, Options, WriteBufferManager}
import org.slf4j.LoggerFactory
import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.rocksdb.BlockBasedTableConfig

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
      getEnv("BUFFER_TIMEOUT").keyedWith("general.buffer.timeout"),
      getEnv("WORKER_MEM").keyedWith("kafkastreams.memory")
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

    val statelessStages = new StatelessStages(settings)
    val statefulStages = new StatefulStages(settings)

    registerCorrectPartialFlowForRun(settings, builder, statelessStages, statefulStages)

    val topology = builder.build(props) // properties need to be passed here to enable optimization of the pipeline
    logger.error("Topology description {}", topology.describe)
    val streams = new KafkaStreams(topology, props)

    streams.start()
    Thread.sleep(10000000)
    streams.close()
  }

  def registerCorrectPartialFlowForRun(settings: BenchmarkSettingsForKafkaStreams,
    builder: StreamsBuilder, statelessStages: StatelessStages, statefulStages: StatefulStages)
  : Unit = settings.general.lastStage match {

    case UNTIL_INGEST =>
      val rawStreams = statelessStages.ingestStage(builder)

      rawStreams.transform(new TransformerSupplier[String, String, KeyValue[String, String]] {
        override def get(): Transformer[String, String, KeyValue[String, String]] = {
          new Transformer[String, String, KeyValue[String, String]] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext): Unit = {
              processorContext = context
            }

            override def transform(key: String, value: String): KeyValue[String, String] = {
              val (parsedKey, parsedValue) = JsonPrinter.jsonFor((processorContext.topic(), value, processorContext.timestamp()), settings.specific.jobProfileKey)
              new KeyValue[String, String](parsedKey, parsedValue)
            }

            override def close(): Unit = {}
          }
        }
      }).to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))


      if (settings.general.shouldPrintOutput) {
        rawStreams.print(Printed.toSysOut[String, String]())
      }
    case UNTIL_PARSE =>
      val rawStreams = statelessStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = statelessStages.parsingStage(rawStreams)

      parsedFlowStream.map { (key: String, obs: FlowObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))
      parsedSpeedStream.map { (key: String, obs: SpeedObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        parsedFlowStream.print(Printed.toSysOut[String, FlowObservation]())
        parsedSpeedStream.print(Printed.toSysOut[String, SpeedObservation]())
      }

    case UNTIL_JOIN =>
      val rawStreams = statelessStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = statelessStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = statefulStages.joinStage(parsedFlowStream, parsedSpeedStream)

      joinedSpeedAndFlowStreams.map { (key: String, obs: AggregatableObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        joinedSpeedAndFlowStreams.print(Printed.toSysOut())
      }

    case UNTIL_TUMBLING_WINDOW =>
      val rawStreams = statelessStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = statelessStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = statefulStages.joinStage(parsedFlowStream, parsedSpeedStream)
      val aggregateStream = statefulStages.aggregationAfterJoinStage(joinedSpeedAndFlowStreams)

      aggregateStream.map { (_, obs: AggregatableObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        aggregateStream.map { (_, obs: AggregatableObservation) => JsonPrinter.jsonFor(obs) }
          .print(Printed.toSysOut())
      }

    case UNTIL_SLIDING_WINDOW =>
      val rawStreams = statelessStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = statelessStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = statefulStages.joinStage(parsedFlowStream, parsedSpeedStream)
      val aggregateStream = statefulStages.aggregationAfterJoinStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream =   statefulStages.slidingWindowAfterAggregationStage(aggregateStream)

      relativeChangeStream.map { (key: String, obs: RelativeChangeObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        val toPrintStream: kstream.KStream[String, String] = relativeChangeStream
          .map { (key: String, obs: RelativeChangeObservation) => JsonPrinter.jsonFor(obs) }
        toPrintStream.print(Printed.toSysOut[String, String]())
      }

    case UNTIL_LOWLEVEL_TUMBLING_WINDOW =>
      val aggregationStore: StoreBuilder[KeyValueStore[String, AggregatableObservation]] = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("lane-aggregator-state-store"),
          CustomObjectSerdes.StringSerde,
          CustomObjectSerdes.AggregatableObservationSerde
        )
        .withCachingEnabled()
      builder.addStateStore(aggregationStore)

      val rawStreams = statelessStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = statelessStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = statefulStages.joinStage(parsedFlowStream, parsedSpeedStream)
      val aggregateStream = statefulStages.lowLevelAggregationAfterJoinStage(joinedSpeedAndFlowStreams)

      aggregateStream.map { (_, obs: AggregatableObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        aggregateStream.map { (_, obs: AggregatableObservation) => JsonPrinter.jsonFor(obs) }
          .print(Printed.toSysOut())
      }

    case UNTIL_LOWLEVEL_SLIDING_WINDOW =>
      val aggregationStore: StoreBuilder[KeyValueStore[String, AggregatableObservation]] = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("lane-aggregator-state-store"),
          CustomObjectSerdes.StringSerde,
          CustomObjectSerdes.AggregatableObservationSerde
        )
        .withCachingEnabled()
      builder.addStateStore(aggregationStore)
      val relativeChangeStore: StoreBuilder[KeyValueStore[String, List[AggregatableObservation]]] = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("relative-change-state-store"),
          CustomObjectSerdes.StringSerde,
          CustomObjectSerdes.AggregatedObservationListSerde
        )
        .withCachingEnabled()
      builder.addStateStore(relativeChangeStore)

      val rawStreams = statelessStages.ingestStage(builder)
      val (parsedFlowStream, parsedSpeedStream) = statelessStages.parsingStage(rawStreams)
      val joinedSpeedAndFlowStreams = statefulStages.joinStage(parsedFlowStream, parsedSpeedStream)
      val aggregateStream = statefulStages.lowLevelAggregationAfterJoinStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream = statefulStages.lowLevelSlidingWindowAfterAggregationStage(aggregateStream)

      relativeChangeStream.map { (key: String, obs: RelativeChangeObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        val toPrintStream: kstream.KStream[String, String] = relativeChangeStream
          .map { (key: String, obs: RelativeChangeObservation) => JsonPrinter.jsonFor(obs) }
        toPrintStream.print(Printed.toSysOut[String, String]())
      }

    case REDUCE_WINDOW_WITHOUT_JOIN =>
      val rawFlowStream = statelessStages.ingestFlowStreamStage(builder)
      val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
      val aggregatedFlowStream = statefulStages.reduceWindowAfterParsingStage(flowStream)
      aggregatedFlowStream.map { (_: Windowed[String], obs: AggregatableFlowObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        aggregatedFlowStream.print(Printed.toSysOut())
      }

    case NON_INCREMENTAL_WINDOW_WITHOUT_JOIN =>
      val persistentKeyValueStore: StoreBuilder[KeyValueStore[String, List[AggregatableFlowObservation]]] = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(settings.specific.nonIncrementalWindowAfterParsingStateStore),
          CustomObjectSerdes.StringSerde,
          CustomObjectSerdes.AggregatableFlowObservationListSerde
        ).withCachingEnabled()
      builder.addStateStore(persistentKeyValueStore)

      val rawFlowStream = statelessStages.ingestFlowStreamStage(builder)
      val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
      val aggregatedFlowStream = statefulStages.nonIncrementalWindowAfterParsingStage(flowStream)
      aggregatedFlowStream.map { (_: String, obs: AggregatableFlowObservation) => JsonPrinter.jsonFor(obs) }
        .to(settings.general.outputTopic)(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.StringSerde))

      if (settings.general.shouldPrintOutput) {
        aggregatedFlowStream.print(Printed.toSysOut())
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
    streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, settings.specific.numStreamsThreadsPerInstance.toString)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, settings.specific.commitInterval.toString)
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, settings.specific.cacheMaxBytesBuffering.toString)
    streamsConfiguration.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, settings.specific.maxTaskIdleMillis)
    streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE) // merges internal topics when possible
    streamsConfiguration.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, (10 * 60 * 1000).toString) // store data for 5 minutes

    if (settings.specific.exactlyOnce) {
      streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    }

    // Producer Config
    streamsConfiguration.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, settings.specific.compressionTypeConfig)
    streamsConfiguration.put(ProducerConfig.BATCH_SIZE_CONFIG, settings.specific.batchSize.toString)
    streamsConfiguration.put(ProducerConfig.LINGER_MS_CONFIG, settings.specific.lingerMs.toString)
    streamsConfiguration.put(ProducerConfig.BUFFER_MEMORY_CONFIG, settings.specific.bufferMemorySize.toString)

    // Consumer Config
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, settings.general.kafkaAutoOffsetReset)
    streamsConfiguration.put(ConsumerConfig.GROUP_ID_CONFIG, settings.specific.applicationId)
    streamsConfiguration.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, settings.specific.fetchMinBytes.toString)

    // For monitoring of memory
    streamsConfiguration.put(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, classOf[JmxReporter].getName)
    streamsConfiguration.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "1000")

    streamsConfiguration.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, classOf[BoundedMemoryRocksDBConfig].getName)

    streamsConfiguration
  }
}

object CustomMemoryRocksDBConfig {
  val rocksDBSize: Long = Math.round(Try(sys.env("WORKER_MEM")).getOrElse("20").toInt * 0.2)
  val blockCacheSize: Long = rocksDBSize * 1024 * 1024 * 1024 // TOTAL_OFF_HEAP_MEMORY
  val blockSize: Long = 32 * 1024L // BLOCK_SIZE 4KB～32KB
  val writeBufferSize: Int = 128 * 1024 * 1024 // MEMTABLE_SIZE: related to the total memory and disk type, typical value is in the range of 32～128MB.
  val maxWriteBufferNumber: Int = 6 // N_MEMTABLES
  val writeBufferManagerBytes: Int = 1024 * 1024 * 1024 // TOTAL_MEMTABLE_MEMORY:
}

class BoundedMemoryRocksDBConfig extends RocksDBConfigSetter {
  private val cache: org.rocksdb.Cache = new org.rocksdb.LRUCache(CustomMemoryRocksDBConfig.blockCacheSize)
  private val writeBufferManager: org.rocksdb.WriteBufferManager = new org.rocksdb.WriteBufferManager(CustomMemoryRocksDBConfig.writeBufferManagerBytes, cache)
  private val filter: org.rocksdb.Filter = new org.rocksdb.BloomFilter(10)

  override def setConfig(storeName: String, options: Options, configs: util.Map[String, AnyRef]): Unit = {
    val tableConfig: BlockBasedTableConfig = new org.rocksdb.BlockBasedTableConfig();

    tableConfig.setBlockCache(cache)
    tableConfig.setBlockSize(CustomMemoryRocksDBConfig.blockSize)
    tableConfig.setCacheIndexAndFilterBlocks(true)
    options.setWriteBufferManager(writeBufferManager)
    tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true)
    tableConfig.setPinTopLevelIndexAndFilter(true)
    options.setMaxWriteBufferNumber(CustomMemoryRocksDBConfig.maxWriteBufferNumber) //nMemTables
    options.setWriteBufferSize(CustomMemoryRocksDBConfig.writeBufferSize) // memtable size
    tableConfig.setFilter(filter)

  }

  override def close(storeName: String, options: Options): Unit = {
    // Cache and WriteBufferManager should not be closed here, as the same objects are shared by every store instance.
    // The filter, however, is not shared and should be closed to avoid leaking memory.
    filter.close();
  }
}