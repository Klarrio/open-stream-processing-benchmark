package flink.benchmark.stages


import java.text.{DateFormat, SimpleDateFormat}

import common.benchmark._
import common.benchmark.input.Parsers
import common.benchmark.stages.InitialStagesTemplate
import common.config.JobExecutionMode.LATENCY_CONSTANT_RATE
import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

import collection.JavaConverters._

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
  def ingestStage(executionEnvironment: StreamExecutionEnvironment): DataStream[(String, String, String, Long)] = {
    val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val kafkaSource = new FlinkKafkaConsumer010[(String, String, String )](
      List(settings.general.flowTopic, settings.general.speedTopic).asJava,
      new RawObservationDeserializer,
      kafkaProperties
    )

    if (settings.general.kafkaAutoOffsetReset.contains("earliest"))
      kafkaSource.setStartFromEarliest()
    else kafkaSource.setStartFromLatest()

    val streams = executionEnvironment
      .addSource(kafkaSource)(createTypeInformation[(String, String, String)])
      .process(new TimestampExtractor)


    val streamsWithWatermarks = streams.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, String, String, Long)] {
      var currentMaxTimestamp: Long = _

      override def extractTimestamp(element: (String, String, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._4
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }

      override def getCurrentWatermark(): Watermark = {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        new Watermark(currentMaxTimestamp - settings.specific.maxOutOfOrderness)
      }
    })


    streamsWithWatermarks
  }


  /**
    * Parses the speed and flow streams
    *
    * @param executionEnvironment Flink's execution environment
    * @return [[DataStream]] of joined [[FlowObservation]] and [[SpeedObservation]]
    */
  def parsingStage(rawStreams: DataStream[(String, String, String, Long)]): (DataStream[FlowObservation], DataStream[SpeedObservation]) = {

    val flowStream = rawStreams
      .filter { event: (String, String, String, Long) =>
        event._2.contains("flow")
      }
      .map { event: (String, String, String, Long) =>
        Parsers.parseLineFlowObservation(event._1, event._3, event._4)._2
      }
    val speedStream = rawStreams
      .filter { event: (String, String, String, Long) =>
        event._2.contains("speed")
      }
      .map { event: (String, String, String, Long) =>
        Parsers.parseLineSpeedObservation(event._1, event._3, event._4)._2
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


/**
  * ProcessFunction: extracts kafka timestamp
  *
  */
class TimestampExtractor extends ProcessFunction[(String, String, String), (String, String, String, Long)] {
  override def open(parameters: Configuration): Unit = {}

  override def processElement(value: (String, String, String), ctx: ProcessFunction[(String, String, String), (String, String, String, Long)]#Context, out: Collector[(String, String, String, Long)]): Unit = {
    out.collect((value._1, value._2, value._3, ctx.timestamp()))
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, String, String), (String, String, String, Long)]#OnTimerContext, out: Collector[(String, String, String, Long)]): Unit = {}
}

/**
  * ProcessFunction: parses lines to [[SpeedObservation]]
  *
  */
class ParseSpeedObservation extends ProcessFunction[(String, String), SpeedObservation] {
  override def open(parameters: Configuration): Unit = {}

  override def processElement(value: (String, String), ctx: ProcessFunction[(String, String), SpeedObservation]#Context, out: Collector[SpeedObservation]): Unit = {
    val (_, speedObservation) = Parsers.parseLineSpeedObservation(value._1, value._2, ctx.timestamp())
    out.collect(speedObservation)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, String), SpeedObservation]#OnTimerContext, out: Collector[SpeedObservation]): Unit = {}
}

/**
  * ProcessFunction: parses lines to [[FlowObservation]]
  *
  */
class ParseFlowObservation extends ProcessFunction[(String, String), FlowObservation] {
  override def open(parameters: Configuration): Unit = {}

  override def processElement(value: (String, String), ctx: ProcessFunction[(String, String), FlowObservation]#Context, out: Collector[FlowObservation]): Unit = {
    val (_, flowObservation) = Parsers.parseLineFlowObservation(value._1, value._2, ctx.timestamp())
    out.collect(flowObservation)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, String), FlowObservation]#OnTimerContext, out: Collector[FlowObservation]): Unit = {}
}
