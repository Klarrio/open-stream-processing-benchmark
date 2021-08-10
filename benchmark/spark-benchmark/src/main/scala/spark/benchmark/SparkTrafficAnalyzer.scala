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
import common.config.LastStage._
import common.benchmark.output.JsonPrinter
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import spark.benchmark.stages.{StatefulStages, StatelessStages, KafkaSinkForSpark}

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

    val kafkaSink: Broadcast[KafkaSinkForSpark] = sparkSession.sparkContext.broadcast(KafkaSinkForSpark(settings.general.kafkaBootstrapServers, settings.general.outputTopic))

    val statelessStages = new StatelessStages(settings, kafkaParams)
    val statefulStages = new StatefulStages(settings)

    registerCorrectPartialFlowForRun(settings, ssc, kafkaSink, statelessStages, statefulStages)

    ssc.checkpoint(settings.specific.checkpointDir)
    ssc.start()
    ssc.awaitTerminationOrTimeout(60000)
  }

  def registerCorrectPartialFlowForRun(settings: BenchmarkSettingsForSpark,
    ssc: StreamingContext, kafkaSink: Broadcast[KafkaSinkForSpark],
    statelessStages: StatelessStages, statefulStages: StatefulStages)
  : Unit = settings.general.lastStage match {

    case UNTIL_INGEST =>
      val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage(ssc)
      rawFlowStream.foreachRDD { rdd => rdd.foreach { r => kafkaSink.value.send(JsonPrinter.jsonFor(r, settings.specific.jobProfileKey)) } }
      rawSpeedStream.foreachRDD { rdd => rdd.foreach { r => kafkaSink.value.send(JsonPrinter.jsonFor(r, settings.specific.jobProfileKey)) } }
      if (settings.general.shouldPrintOutput) {
        rawFlowStream.print()
        rawSpeedStream.print()
      }

    case UNTIL_PARSE =>
      val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage(ssc)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)

      flowStream.foreachRDD { rdd =>
        rdd.foreach { case (key: String, obs: FlowObservation) =>
          kafkaSink.value.send(JsonPrinter.jsonFor(obs))
        }
      }
      speedStream.foreachRDD { rdd =>
        rdd.foreach { case (key: String, obs: SpeedObservation) =>
          kafkaSink.value.send(JsonPrinter.jsonFor(obs))
        }
      }
      if (settings.general.shouldPrintOutput) {
        flowStream.print()
        speedStream.print()
      }

    case UNTIL_JOIN =>
      val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage(ssc)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams = statefulStages.joinStage(flowStream, speedStream)

      joinedSpeedAndFlowStreams.foreachRDD { rdd =>
        rdd.foreach { case (key: String, (flow: FlowObservation, speed: SpeedObservation)) =>
          val obs = new AggregatableObservation(flow, speed)
          kafkaSink.value.send(JsonPrinter.jsonFor(obs))
        }
      }
      if (settings.general.shouldPrintOutput) {
        joinedSpeedAndFlowStreams.print()
      }

    case UNTIL_TUMBLING_WINDOW =>
      val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage(ssc)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams: DStream[(String, (FlowObservation, SpeedObservation))] = statefulStages.joinStage(flowStream, speedStream)
      val aggregateStream: DStream[AggregatableObservation] = statefulStages.aggregationAfterJoinStage(joinedSpeedAndFlowStreams)

      aggregateStream.foreachRDD { rdd =>
        rdd.foreach { obs =>
          kafkaSink.value.send(JsonPrinter.jsonFor(obs))
        }
      }

      if (settings.general.shouldPrintOutput) {
        aggregateStream.print()
      }

    case UNTIL_SLIDING_WINDOW =>
      val (rawFlowStream, rawSpeedStream) = statelessStages.ingestStage(ssc)
      val (flowStream, speedStream) = statelessStages.parsingStage(rawFlowStream, rawSpeedStream)
      val joinedSpeedAndFlowStreams = statefulStages.joinStage(flowStream, speedStream)
      val aggregateStream = statefulStages.aggregationAfterJoinStage(joinedSpeedAndFlowStreams)
      val relativeChangeStream = statefulStages.slidingWindowAfterAggregationStage(aggregateStream)

      relativeChangeStream.foreachRDD { rdd =>
        rdd.foreach { obs =>
          kafkaSink.value.send(JsonPrinter.jsonFor(obs))
        }
      }

      if (settings.general.shouldPrintOutput) {
        relativeChangeStream.print()
      }

    case REDUCE_WINDOW_WITHOUT_JOIN =>
      val rawFlowStream = statelessStages.ingestFlowStreamStage(ssc)
      val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
      val aggregatedFlowStream = statefulStages.reduceWindowAfterParsingStage(flowStream)

      aggregatedFlowStream.foreachRDD { rdd =>
        rdd.foreach { obs =>
          kafkaSink.value.send(JsonPrinter.jsonFor(obs._2))
        }
      }

      if (settings.general.shouldPrintOutput) {
        aggregatedFlowStream.print()
      }

    case NON_INCREMENTAL_WINDOW_WITHOUT_JOIN =>
      val rawFlowStream = statelessStages.ingestFlowStreamStage(ssc)
      val flowStream = statelessStages.parsingFlowStreamStage(rawFlowStream)
      val aggregatedFlowStream = statefulStages.nonIncrementalWindowAfterParsingStage(flowStream)

      aggregatedFlowStream.foreachRDD { rdd =>
        rdd.foreach { obs =>
          kafkaSink.value.send(JsonPrinter.jsonFor(obs._2))
        }
      }

      if (settings.general.shouldPrintOutput) {
        aggregatedFlowStream.print()
      }
  }

  /**
   * Initializes Spark
   *
   * @param settings Spark configuration properties
   * @return Spark session and streaming context
   */
  def initSpark(settings: BenchmarkSettingsForSpark): (SparkSession, StreamingContext) = {
    val sparkSession = SparkSession.builder()
      .master(settings.specific.sparkMaster)
      .appName("spark-streaming-benchmark")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryo.registrationRequired", "true")
      .config(getKryoConfig)
      .config("spark.streaming.receiver.writeAheadLog.enable", settings.specific.writeAheadLogEnabled)
      .config("spark.locality.wait", settings.specific.localityWait)
      .config("spark.streaming.blockInterval", settings.specific.blockInterval)
      .config("spark.sql.streaming.minBatchesToRetain", 2)
      .getOrCreate()

    val ssc = new StreamingContext(sparkSession.sparkContext, Milliseconds(settings.specific.batchInterval))
    (sparkSession, ssc)
  }

  /**
   * Initializes Kafka
   *
   * @param settings Spark configuration properties
   * @return [[Map]] containing Kafka configuration properties
   */
  def initKafka(settings: BenchmarkSettingsForSpark): Map[String, Object] = {
    val groupId = "SPARK"

    Map[String, Object](
      "bootstrap.servers" -> settings.general.kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> settings.general.kafkaAutoOffsetReset,
      "enable.auto.commit" -> Boolean.box(false)
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
        classOf[KafkaSinkForSpark],
        classOf[spark.benchmark.stages.KafkaSinkForSpark],
        classOf[Array[common.benchmark.RelativeChangeObservation]],
        classOf[scala.collection.mutable.WrappedArray$ofRef]
      )
    )
  }
}
