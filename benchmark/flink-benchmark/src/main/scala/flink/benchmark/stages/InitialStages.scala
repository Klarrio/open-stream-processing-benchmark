package flink.benchmark.stages


import common.benchmark._
import common.benchmark.input.Parsers
import common.benchmark.stages.InitialStagesTemplate
import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

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
class InitialStages(
  settings: BenchmarkSettingsForFlink,
  kafkaProperties: java.util.Properties
) extends Serializable with InitialStagesTemplate {

  /**
    * Ingests the flow and speed data from Kafka
    *
    * @param executionEnvironment
    * @return
    */
  def ingestStage(executionEnvironment: StreamExecutionEnvironment): (DataStream[(String, String, Long)], DataStream[(String, String, Long)]) = {
    val flowKafkaSource = new FlinkKafkaConsumer[(String, String, Long)](
      List(settings.general.flowTopic).asJava,
      new RawObservationDeserializer,
      kafkaProperties
    )

    if (settings.general.kafkaAutoOffsetReset.contains("earliest"))
      flowKafkaSource.setStartFromEarliest()
    else flowKafkaSource.setStartFromLatest()

    val rawFlowStream = executionEnvironment
      .addSource(flowKafkaSource)(createTypeInformation[(String, String, Long)])
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, String, Long)] {
        var currentMaxTimestamp: Long = _

        override def extractTimestamp(element: (String, String, Long), previousElementTimestamp: Long): Long = {
          val timestamp = element._3
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          timestamp
        }

        override def getCurrentWatermark(): Watermark = {
          // return the watermark as current highest timestamp minus the out-of-orderness bound
          new Watermark(currentMaxTimestamp - settings.specific.maxOutOfOrderness)
        }
      })

    val speedKafkaSource = new FlinkKafkaConsumer[(String, String, Long)](
      List(settings.general.speedTopic).asJava,
      new RawObservationDeserializer,
      kafkaProperties
    )

    if (settings.general.kafkaAutoOffsetReset.contains("earliest"))
      speedKafkaSource.setStartFromEarliest()
    else speedKafkaSource.setStartFromLatest()

    val rawSpeedStream = executionEnvironment
      .addSource(speedKafkaSource)(createTypeInformation[(String, String, Long)])
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, String, Long)] {
        var currentMaxTimestamp: Long = _

        override def extractTimestamp(element: (String, String, Long), previousElementTimestamp: Long): Long = {
          val timestamp = element._3
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          timestamp
        }

        override def getCurrentWatermark(): Watermark = {
          // return the watermark as current highest timestamp minus the out-of-orderness bound
          new Watermark(currentMaxTimestamp - settings.specific.maxOutOfOrderness)
        }
      })


    (rawFlowStream, rawSpeedStream)
  }


  /**
    * Parses the speed and flow streams
    *
    * @param executionEnvironment Flink's execution environment
    * @return [[DataStream]] of joined [[FlowObservation]] and [[SpeedObservation]]
    */
  def parsingStage(rawFlowStream: DataStream[(String, String, Long)], rawSpeedStream: DataStream[(String, String, Long)]): (DataStream[FlowObservation], DataStream[SpeedObservation]) = {

    val flowStream = rawFlowStream
      .map { event: (String, String, Long) =>
        Parsers.parseLineFlowObservation(event._1, event._2, event._3)._2
      }
    val speedStream = rawSpeedStream
      .map { event: (String, String, Long) =>
        Parsers.parseLineSpeedObservation(event._1, event._2, event._3)._2
      }

    (flowStream, speedStream)
  }

  /**
    * Joins the flow and speed streams
    *
    * @param parsedFlowStream  [[DataStream]] of [[FlowObservation]]
    * @param parsedSpeedStream [[DataStream]] of [[SpeedObservation]]
    * @return [[DataStream]] of [[FlowObservation]] and [[SpeedObservation]]
    */
  def joinStage(parsedFlowStream: DataStream[FlowObservation], parsedSpeedStream: DataStream[SpeedObservation]): DataStream[AggregatableObservation] = {
    val joinedStream = parsedFlowStream.keyBy(flowObservation => (flowObservation.measurementId, flowObservation.internalId, flowObservation.timestamp))
      .intervalJoin(parsedSpeedStream.keyBy(speedObservation => (speedObservation.measurementId, speedObservation.internalId, speedObservation.timestamp)))
      .between(Time.milliseconds(-settings.general.publishIntervalMillis.toLong), Time.milliseconds(settings.general.publishIntervalMillis.toLong))
      .process[AggregatableObservation] {
      new ProcessJoinFunction[FlowObservation, SpeedObservation, AggregatableObservation] {
        override def processElement(flowObservation: FlowObservation, speedObservation: SpeedObservation, context: ProcessJoinFunction[FlowObservation, SpeedObservation, AggregatableObservation]#Context, collector: Collector[AggregatableObservation]): Unit = {
          collector.collect(new AggregatableObservation(flowObservation, speedObservation))
        }
      }
    }

    joinedStream
  }
}