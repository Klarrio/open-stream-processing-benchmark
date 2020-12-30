package kafka.benchmark.stages

import java.time.Duration

import common.benchmark._
import common.benchmark.input.Parsers
import common.benchmark.stages.StatelessStagesTemplate
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
 * @param settings Kafka-streams configuration properties
 */
class StatelessStages(settings: BenchmarkSettingsForKafkaStreams)
  extends Serializable with StatelessStagesTemplate {

  /**
   * Ingests the flow and speed data from Kafka
   *
   * @param builder
   * @return raw speed and flow events
   */
  def ingestStage(builder: StreamsBuilder): KStream[String, String] = {
    // both topics need to be read in together otherwise the synchronization of bursts goes wrong
    val rawStreams: KStream[String, String] = builder.stream[String, String](Set(settings.general.flowTopic, settings.general.speedTopic))

    rawStreams
  }


  /**
   * Parses the flow and speed events
   *
   * @return [[KStream]] of [[FlowObservation]] and [[KStream]] of [[SpeedObservation]]
   */
  def parsingStage(rawStreams: KStream[String, String]): (KStream[String, FlowObservation], KStream[String, SpeedObservation]) = {

    val flowStream: KStream[String, FlowObservation] = rawStreams.filter { case (key, value) => value.contains("flow") }
      .transformValues(new ValueTransformerWithKeySupplier[String, String, FlowObservation] {
        override def get(): ValueTransformerWithKey[String, String, FlowObservation] = {
          new ValueTransformerWithKey[String, String, FlowObservation] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext) = {
              processorContext = context
            }

            override def transform(key: String, value: String): FlowObservation = {
              Parsers.parseLineFlowObservation(key, value, processorContext.timestamp(), settings.specific.jobProfileKey)
            }

            override def close(): Unit = {}
          }
        }
      })

    val speedStream: KStream[String, SpeedObservation] = rawStreams.filter { case (key, value) => value.contains("speed") }
      .transformValues(new ValueTransformerWithKeySupplier[String, String, SpeedObservation] {
        override def get(): ValueTransformerWithKey[String, String, SpeedObservation] = {
          new ValueTransformerWithKey[String, String, SpeedObservation] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext) = {
              processorContext = context
            }

            override def transform(key: String, value: String): SpeedObservation = {
              Parsers.parseLineSpeedObservation(key, value, processorContext.timestamp(), settings.specific.jobProfileKey)
            }

            override def close(): Unit = {}
          }
        }
      })

    (flowStream, speedStream)
  }

  /**
   * Ingests the flow data from Kafka
   *
   * @param builder
   * @return raw flow events
   */
  def ingestFlowStreamStage(builder: StreamsBuilder): KStream[String, String] = {
    // both topics need to be read in together otherwise the synchronization of bursts goes wrong
    val rawStreams: KStream[String, String] = builder.stream[String, String](Set(settings.general.flowTopic))

    rawStreams
  }

  /**
   * Parses the flow and speed events
   *
   * @return [[KStream]] of [[FlowObservation]] and [[KStream]] of [[SpeedObservation]]
   */
  def parsingFlowStreamStage(rawStreams: KStream[String, String]): KStream[String, FlowObservation] = {

    val flowStream: KStream[String, FlowObservation] = rawStreams.filter { case (key, value) => value.contains("flow") }
      .transformValues(new ValueTransformerWithKeySupplier[String, String, FlowObservation] {
        override def get(): ValueTransformerWithKey[String, String, FlowObservation] = {
          new ValueTransformerWithKey[String, String, FlowObservation] {
            var processorContext: ProcessorContext = _

            override def init(context: ProcessorContext) = {
              processorContext = context
            }

            override def transform(key: String, value: String): FlowObservation = {
              Parsers.parseLineFlowObservation(key, value, processorContext.timestamp(), settings.specific.jobProfileKey)
            }

            override def close(): Unit = {}
          }
        }
      })

    flowStream
  }

}