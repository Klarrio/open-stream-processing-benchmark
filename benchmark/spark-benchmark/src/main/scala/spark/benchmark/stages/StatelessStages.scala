package spark.benchmark.stages

import common.benchmark.input.Parsers
import common.benchmark.stages.StatelessStagesTemplate
import common.benchmark.{FlowObservation, SpeedObservation}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import spark.benchmark.BenchmarkSettingsForSpark

/**
 * Contains all methods required for parsing and joining the incoming streams
 *
 * - Reads in from Kafka
 * - Parse to case classes [[FlowObservation]] and [[SpeedObservation]]
 * - Join the flow and speed observations together for each timestamp and measurement ID
 *
 * @param settings    Spark configuration properties
 * @param kafkaParams Kafka configuration properties
 */
class StatelessStages(settings: BenchmarkSettingsForSpark, kafkaParams: Map[String, Object])
  extends Serializable with StatelessStagesTemplate {

  /**
   * Consumes from Kafka from flow and speed topic
   *
   * @param ssc streaming context, passed on method instead of class to avoid kafka ConcurrentModificationException
   * @return raw kafka stream
   */
  def ingestStage(ssc: StreamingContext): (DStream[(String, String, String, Long)], DStream[(String, String, String, Long)]) = {
    val timeToString = "SPARK/" + System.currentTimeMillis()

    val kafkaParameters = Map[String, Object](
      "bootstrap.servers" -> settings.general.kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> timeToString,
      "auto.offset.reset" -> settings.general.kafkaAutoOffsetReset,
      "enable.auto.commit" -> Boolean.box(true)
    )
    val preferredHosts = LocationStrategies.PreferConsistent
    val flowStream = KafkaUtils.createDirectStream[String, String](ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](List(settings.general.flowTopic), kafkaParameters)
    )
    .map { r: ConsumerRecord[String, String] => (r.topic(), r.key(), r.value(), r.timestamp()) }

    val speedStream = KafkaUtils.createDirectStream[String, String](ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](List(settings.general.speedTopic), kafkaParameters)
    ).map { r: ConsumerRecord[String, String] => (r.topic(), r.key(), r.value(), r.timestamp()) }

    (flowStream, speedStream)
  }

  /**
   * Parses data from both streams
   * @param rawFlowStream [[DStream]] that contains the raw data for the flow observation
   * @param rawSpeedStream [[DStream]] that contains the raw data for the speed observation
   * @return [[DStream]] of [[FlowObservation]] and [[DStream]] of [[SpeedObservation]]
   */
  def parsingStage(rawFlowStream: DStream[(String, String, String, Long)], rawSpeedStream: DStream[(String, String, String, Long)]):
  (DStream[(String, FlowObservation)], DStream[(String, SpeedObservation)]) = {
    val parsedFlowStream = rawFlowStream.map {
      case (_: String, key: String, value: String, publishTime: Long) =>
        val flowObservation = Parsers.parseLineFlowObservation(key, value, publishTime, settings.specific.jobProfileKey)
        val keyOfObservation = flowObservation.measurementId + "/" + flowObservation.internalId + "/" + flowObservation.roundedTimestamp
        (keyOfObservation, flowObservation)
    }

    val parsedSpeedStream = rawSpeedStream.map {
      case (_: String, key: String, value: String, publishTime: Long) =>
        val speedObservation = Parsers.parseLineSpeedObservation(key, value, publishTime, settings.specific.jobProfileKey)
        val keyOfObservation = speedObservation.measurementId + "/" + speedObservation.internalId + "/" + speedObservation.roundedTimestamp
        (keyOfObservation, speedObservation)
    }

    (parsedFlowStream, parsedSpeedStream)
  }

  /**
   * Consumes from Kafka from flow topic
   *
   * @param ssc streaming context, passed on method instead of class to avoid kafka ConcurrentModificationException
   * @return raw flow stream
   */
  def ingestFlowStreamStage(ssc: StreamingContext): DStream[(String, String, String, Long)] = {
    val timeToString = "SPARK/" + System.currentTimeMillis()

    val kafkaParameters = Map[String, Object](
      "bootstrap.servers" -> settings.general.kafkaBootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> timeToString,
      "auto.offset.reset" -> settings.general.kafkaAutoOffsetReset,
      "enable.auto.commit" -> Boolean.box(true)
    )
    val preferredHosts = LocationStrategies.PreferConsistent
    val flowStream = KafkaUtils.createDirectStream[String, String](ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](List(settings.general.flowTopic), kafkaParameters)
    )
      .map { r: ConsumerRecord[String, String] => (r.topic(), r.key(), r.value(), r.timestamp()) }

    flowStream
  }

  /**
   * Parses data from flow stream
   * @param rawFlowStream [[DStream]] that contains the raw data for the flow observation
   * @return [[DStream]] of [[FlowObservation]]
   */
  def parsingFlowStreamStage(rawFlowStream: DStream[(String, String, String, Long)]): DStream[(String, FlowObservation)] = {
    val parsedFlowStream = rawFlowStream.map {
      case (_: String, key: String, value: String, publishTime: Long) =>
        val flowObservation = Parsers.parseLineFlowObservation(key, value, publishTime, settings.specific.jobProfileKey)
        val keyOfObservation = flowObservation.measurementId + "/" + flowObservation.internalId + "/" + flowObservation.roundedTimestamp
        (keyOfObservation, flowObservation)
    }

    parsedFlowStream
  }
}