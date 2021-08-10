package kafka.benchmark.stages

import java.time.Duration
import java.util.Properties

import common.benchmark.{AggregatableFlowObservation, FlowObservation}
import common.utils.TestObservations
import kafka.benchmark.{BenchmarkSettingsForKafkaStreams, KafkaTrafficAnalyzer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._

class NonIncrementalWindowAfterParsingStageTest  extends FlatSpec with Matchers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val overrides: Map[String, Any] = Map("general.last.stage" -> "5",
    "general.window.after.parsing.window.duration" -> 300000,
    "general.window.after.parsing.slide.duration" -> 60000)
  val settings: BenchmarkSettingsForKafkaStreams = new BenchmarkSettingsForKafkaStreams(overrides)

  val props: Properties = KafkaTrafficAnalyzer.initKafka(settings)
  val builder = new StreamsBuilder()

  val persistentKeyValueStore: StoreBuilder[KeyValueStore[String, List[AggregatableFlowObservation]]] = Stores
    .keyValueStoreBuilder(Stores.persistentKeyValueStore(settings.specific.nonIncrementalWindowAfterParsingStateStore),
      CustomObjectSerdes.StringSerde,
      CustomObjectSerdes.AggregatableFlowObservationListSerde
    ).withCachingEnabled()
  builder.addStateStore(persistentKeyValueStore)

  val statefulStages = new StatefulStages(settings)
  val expectedOutput: Seq[AggregatableFlowObservation] = TestObservations.outputWindowAfterParsingStage.flatten
    .map(_._2)
    .sortBy { f: AggregatableFlowObservation => (f.measurementId, f.publishTimestamp) }

  "window after parsing stage" should " produce correct output" in {
    val inputStream = builder.stream("input-topic")(Consumed.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.FlowObservationSerde))

    statefulStages.nonIncrementalWindowAfterParsingStage(inputStream)
      .map[String, AggregatableFlowObservation] { (key: String, obs: AggregatableFlowObservation) => (obs.measurementId, obs) }
      .to("output-topic")(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableFlowObservationSerde))

    val topology = builder.build()
    val topologyTestDriver = new TopologyTestDriver(topology, props)

    val inputTopic = topologyTestDriver.createInputTopic[String, FlowObservation]("input-topic", new StringSerializer, new FlowSerializer)
    val outputTopic = topologyTestDriver.createOutputTopic[String, AggregatableFlowObservation]("output-topic", new StringDeserializer, new AggregatableFlowDeserializer)

    TestObservations.flowObservationsAfterParsingStage
      .foreach { next =>
        next.foreach{ obs =>
          inputTopic.pipeInput(obs._1, obs._2, obs._2.publishTimestamp)
        }
      }

    val myOutputList = outputTopic.readValuesToList().asScala
      .sortBy { f: AggregatableFlowObservation => (f.measurementId, f.publishTimestamp) }

    println(myOutputList.mkString("\n"))

    println("Expected output")
    println(expectedOutput.mkString("\n"))
    myOutputList should contain allElementsOf expectedOutput
    topologyTestDriver.close()
  }
}