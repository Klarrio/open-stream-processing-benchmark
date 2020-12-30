package flink.benchmark.stages


import java.time.Duration

import common.benchmark._
import common.benchmark.input.Parsers
import common.benchmark.stages.StatelessStagesTemplate
import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.JavaConverters._

/**
 * Contains all methods required for parsing and joining the incoming streams
 *
 * - Reads in from Kafka
 * - Parse to case classes [[FlowObservation]] and [[SpeedObservation]]
 * - Join the flow and speed observations together for each timestamp and measurement ID
 *
 * @param settings        Flink configuration properties
 * @param kafkaProperties Kafka configuration properties
 */
class StatelessStages(
  settings: BenchmarkSettingsForFlink,
  kafkaProperties: java.util.Properties
) extends Serializable with StatelessStagesTemplate {

  /**
   * Ingests the flow and speed data from Kafka
   *
   * @param executionEnvironment
   * @return
   */
  def ingestStage(executionEnvironment: StreamExecutionEnvironment): DataStream[(String, String, Long)] = {

    val kafkaSource: FlinkKafkaConsumer[(String, String, Long)] = new FlinkKafkaConsumer[(String, String, Long)](
      List(settings.general.speedTopic, settings.general.flowTopic).asJava,
      new RawObservationDeserializer,
      kafkaProperties
    )

    kafkaSource.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[(String, String, Long)](Duration.ofMillis(settings.specific.maxOutOfOrderness))
    )

    if (settings.general.kafkaAutoOffsetReset.contains("earliest")) {
      kafkaSource.setStartFromEarliest()
    } else {
      kafkaSource.setStartFromLatest()
    }

    val rawStream = executionEnvironment
      .addSource(kafkaSource)(createTypeInformation[(String, String, Long)])

    rawStream
  }

  /**
   * Parses the speed and flow streams
   *
   * @param rawStream raw flow and speed events as they were ingested
   * @return [[DataStream]] of joined [[FlowObservation]] and [[SpeedObservation]]
   */
  def parsingStage(rawStream: DataStream[(String, String, Long)]): (DataStream[FlowObservation], DataStream[SpeedObservation]) = {

    val flowStream = rawStream.filter(_._2.contains("flow"))
      .map { event: (String, String, Long) =>
        Parsers.parseLineFlowObservation(event._1, event._2, event._3, settings.specific.jobProfileKey)
      }
    val speedStream = rawStream.filter(_._2.contains("speed"))
      .map { event: (String, String, Long) =>
        Parsers.parseLineSpeedObservation(event._1, event._2, event._3, settings.specific.jobProfileKey)
      }

    (flowStream, speedStream)
  }

  /**
   * Ingests the flow data from Kafka
   *
   * @param executionEnvironment
   * @return
   */
  def ingestFlowStreamStage(executionEnvironment: StreamExecutionEnvironment): DataStream[(String, String, Long)] = {

    val kafkaSource: FlinkKafkaConsumer[(String, String, Long)] = new FlinkKafkaConsumer[(String, String, Long)](
      settings.general.flowTopic,
      new RawObservationDeserializer,
      kafkaProperties
    )

    kafkaSource.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[(String, String, Long)](Duration.ofMillis(settings.specific.maxOutOfOrderness))
    )

    if (settings.general.kafkaAutoOffsetReset.contains("earliest")) {
      kafkaSource.setStartFromEarliest()
    } else {
      kafkaSource.setStartFromLatest()
    }

    val rawStream = executionEnvironment
      .addSource(kafkaSource)(createTypeInformation[(String, String, Long)])

    rawStream
  }

  /**
   * Parses the speed and flow streams
   *
   * @param rawFlowStream raw flow events as they were ingested
   * @return [[DataStream]] parsed [[FlowObservation]] stream
   */
  def parsingFlowStreamStage(rawFlowStream: DataStream[(String, String, Long)]): DataStream[FlowObservation] = {

    val flowStream = rawFlowStream
      .map { event: (String, String, Long) =>
        Parsers.parseLineFlowObservation(event._1, event._2, event._3, settings.specific.jobProfileKey)
      }

    flowStream
  }

}