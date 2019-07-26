package kafka.benchmark.stages

import java.time.Duration

import common.benchmark._
import common.benchmark.input.Parsers
import common.benchmark.stages.InitialStagesTemplate
import kafka.benchmark.BenchmarkSettingsForKafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.KStream

/**
  * Contains all methods required for parsing and joining the incoming streams
  *
  * - Reads in from Kafka
  * - Parse to case classes [[FlowObservation]] and [[SpeedObservation]]
  * - Join the flow and speed observations together for each timestamp and measurement ID
  *
  * @param kafkaStreamsConfig Kafka-streams configuration properties
  */
class InitialStages(settings: BenchmarkSettingsForKafkaStreams)
  extends Serializable with InitialStagesTemplate {

  /**
    * Ingests the flow and speed data from Kafka
    *
    * @param builder
    * @return raw speed and flow events
    */
  def ingestStage(builder: StreamsBuilder): KStream[String, String] = {
    val rawStreams: KStream[String, String] = builder.stream[String, String](Set(settings.general.flowTopic, settings.general.speedTopic))

    rawStreams
  }


  /**
    * Parses the flow and speed events
    *
    * @param builder kafka-streams builders
    * @return [[KStream]] of [[FlowObservation]] and [[KStream]] of [[SpeedObservation]]
    */
  def parsingStage(rawStreams: KStream[String, String]): (KStream[String, FlowObservation], KStream[String, SpeedObservation]) = {
    val flowStream: KStream[String, FlowObservation] = rawStreams.filter { case (key, value) => value.contains("flow") }
      .transform(new TransformerSupplier[String, String, KeyValue[String, FlowObservation]] {
        override def get(): Transformer[String, String, KeyValue[String, FlowObservation]] = {
          new Transformer[String, String, KeyValue[String, FlowObservation]] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext) = {
              processorContext = context
            }

            override def transform(key: String, value: String): KeyValue[String, FlowObservation] = {
              val (parsedKey, parsedValue) = Parsers.parseLineFlowObservation(key, value, processorContext.timestamp())
              new KeyValue(parsedKey, parsedValue)
            }

            override def close(): Unit = {}
          }
        }
      })

    val speedStream: KStream[String, SpeedObservation] = rawStreams.filter { case (key, value) => value.contains("speed") }
      .transform(new TransformerSupplier[String, String, KeyValue[String, SpeedObservation]] {
        override def get(): Transformer[String, String, KeyValue[String, SpeedObservation]] = {
          new Transformer[String, String, KeyValue[String, SpeedObservation]] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext) = {
              processorContext = context
            }

            override def transform(key: String, value: String): KeyValue[String, SpeedObservation] = {
              val (parsedKey, parsedValue) = Parsers.parseLineSpeedObservation(key, value, processorContext.timestamp())
              new KeyValue(parsedKey, parsedValue)
            }

            override def close(): Unit = {}
          }
        }
      })

    (flowStream, speedStream)
  }

  /**
    * Joins the flow and speed streams
    *
    * @param parsedFlowStream  [[KStream]] of [[FlowObservation]]
    * @param parsedSpeedStream [[KStream]] of [[SpeedObservation]]
    * @return [[KStream]] of [[FlowObservation]] and [[SpeedObservation]]
    */
  def joinStage(parsedFlowStream: KStream[String, FlowObservation], parsedSpeedStream: KStream[String, SpeedObservation]): KStream[String, AggregatableObservation] = {
    val joinedSpeedAndFlowStreams = parsedFlowStream.join(parsedSpeedStream)({
      (v1: FlowObservation, v2: SpeedObservation) => new AggregatableObservation(v1, v2)
    }, JoinWindows.of(Duration.ofMillis(settings.general.publishIntervalMillis))
      .grace(Duration.ofMillis(settings.specific.gracePeriodMillis)))(Joined
      .`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.FlowObservationSerde, CustomObjectSerdes.SpeedObservationSerde))

    joinedSpeedAndFlowStreams
  }

}