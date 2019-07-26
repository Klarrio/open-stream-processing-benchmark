package spark.benchmark.stages

import common.benchmark.input.Parsers
import common.benchmark.stages.InitialStagesTemplate
import common.benchmark.{FlowObservation, SpeedObservation}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
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
class InitialStages(settings: BenchmarkSettingsForSpark, kafkaParams: Map[String, Object])
  extends Serializable with InitialStagesTemplate {

  /**
    * Consumes from Kafka from flow topic
    *
    * @param ssc streaming context, passed on method instead of class to avoid kafka ConcurrentModificationException
    * @return raw kafka stream
    */
  def ingestStage(ssc: StreamingContext): (DStream[(String, String, String, Long)], DStream[(String, String, String, Long)]) = {
    val flowStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](
      Array(settings.general.flowTopic),
      kafkaParams
    )).map { r: ConsumerRecord[String, String] => (r.topic(), r.key(), r.value(), r.timestamp()) }

    val speedStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](
      Array(settings.general.speedTopic),
      kafkaParams
    )).map { r: ConsumerRecord[String, String] => (r.topic(), r.key(), r.value(), r.timestamp()) }
    (flowStream, speedStream)
  }

  /**
    * Parses data from both streams
    *
    * @param ssc streaming context, passed on method instead of class to avoid kafka ConcurrentModificationException
    * @return [[DStream]] of [[FlowObservation]] and [[DStream]] of [[SpeedObservation]]
    */
  def parsingStage(rawFlowStream: DStream[(String, String, String, Long)], rawSpeedStream: DStream[(String, String, String, Long)]):
  (DStream[(String, FlowObservation)], DStream[(String, SpeedObservation)]) = {
    val parsedFlowStream = rawFlowStream.map {
        case (_: String, key: String, value: String, publishTime: Long) =>
          Parsers.parseLineFlowObservation(key, value, publishTime)
      }

    val parsedSpeedStream = rawSpeedStream.map {
        case (_: String, key: String, value: String, publishTime: Long) =>
          Parsers.parseLineSpeedObservation(key, value, publishTime)
      }

    (parsedFlowStream, parsedSpeedStream)
  }

  /**
    * Joins the flow and speed streams
    *
    * If this is the last stage to execute, it publishes the observation on Kafka
    *
    * @param parsedFlowStream  [[DStream]] of [[FlowObservation]]
    * @param parsedSpeedStream [[DStream]] of [[SpeedObservation]]
    * @return [[DStream]] of [[FlowObservation]] and [[SpeedObservation]]
    */

  def joinStage(parsedFlowStream: DStream[(String, FlowObservation)], parsedSpeedStream: DStream[(String, SpeedObservation)]): DStream[(String, (FlowObservation, SpeedObservation))] = {
    parsedFlowStream.join(parsedSpeedStream)
  }


}