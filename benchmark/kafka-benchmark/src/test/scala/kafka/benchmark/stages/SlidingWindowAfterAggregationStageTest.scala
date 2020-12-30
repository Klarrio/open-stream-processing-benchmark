package kafka.benchmark.stages

import java.util.Properties
import java.time.Duration

import common.benchmark.{AggregatableObservation, RelativeChangeObservation}
import common.utils.TestObservations
import kafka.benchmark.stages.{AggregatableObservationSerializer, CustomObjectSerdes, RelativeChangeDeserializer, StatefulStages}
import kafka.benchmark.{BenchmarkSettingsForKafkaStreams, KafkaTrafficAnalyzer}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}
import io.circe.generic.auto._
import io.circe.syntax._

import collection.JavaConverters._

class SlidingWindowAfterAggregationStageTest extends FlatSpec with Matchers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val settings: BenchmarkSettingsForKafkaStreams = new BenchmarkSettingsForKafkaStreams

  val props: Properties = KafkaTrafficAnalyzer.initKafka(settings)
  val builder = new StreamsBuilder()

  val persistentKeyValueStore: StoreBuilder[KeyValueStore[String, List[AggregatableObservation]]] = Stores
    .keyValueStoreBuilder(Stores.persistentKeyValueStore("relative-change-state-store"),
      CustomObjectSerdes.StringSerde,
      CustomObjectSerdes.AggregatedObservationListSerde
    )
    .withCachingEnabled()

  val statefulStages = new StatefulStages(settings)

  val expectedOutput: Seq[RelativeChangeObservation] = TestObservations.observationsAfterRelativeChangeStage.flatten
    .sortBy { f: RelativeChangeObservation => (f.publishTimestamp, f.measurementId) }

  "relative change stage" should " produce correct output" in {
    builder.addStateStore(persistentKeyValueStore)
    val inputStream = builder.stream("input-topic")(Consumed.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
      .map((key, value) => (new Windowed[String](key, new Window(value.roundedTimestamp, value.roundedTimestamp + 1000) {
        override def overlap(other: Window): Boolean = false
      }), value))
    statefulStages.slidingWindowAfterAggregationStage(inputStream)
      .map[String, RelativeChangeObservation] { (key: String, obs: RelativeChangeObservation) => (obs.measurementId, obs) }
      .to("output-topic")(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.RelativeChangeObservationSerde))

    val topology = builder.build()
    val topologyTestDriver = new TopologyTestDriver(topology, props)

    val inputTopic = topologyTestDriver.createInputTopic[String, AggregatableObservation]("input-topic", new StringSerializer, new AggregatableObservationSerializer)
    val outputTopic = topologyTestDriver.createOutputTopic[String, RelativeChangeObservation]("output-topic", new StringDeserializer, new RelativeChangeDeserializer)

    TestObservations.observationsInputRelativeChangeStage.foreach { next =>
      next.distinct.foreach { obs =>
        logger.info("piping input: " + obs.asJson.noSpaces)
        inputTopic.pipeInput(obs.measurementId, obs, obs.publishTimestamp)
      }
      inputTopic.advanceTime(Duration.ofMillis(1000))
    }

    val myOutput = outputTopic.readValuesToList().asScala
      .sortBy { f: RelativeChangeObservation => (f.publishTimestamp, f.measurementId) }

    myOutput should contain allElementsOf expectedOutput
    topologyTestDriver.close()
  }
}
