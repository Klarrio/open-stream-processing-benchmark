package kafka.benchmark.stages

import java.util.Properties

import common.benchmark.AggregatableObservation
import common.utils.TestObservations
import kafka.benchmark.stages.{AggregatableObservationDeserializer, AggregatableObservationSerializer, CustomObjectSerdes, StatefulStages}
import kafka.benchmark.{BenchmarkSettingsForKafkaStreams, KafkaTrafficAnalyzer}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite, Matchers}

import collection.JavaConverters._

class AggregationStageTest extends FlatSpec with Matchers with BeforeAndAfter {
  val settings: BenchmarkSettingsForKafkaStreams = new BenchmarkSettingsForKafkaStreams
  val props: Properties = KafkaTrafficAnalyzer.initKafka(settings)
  val builder = new StreamsBuilder()

  val persistentKeyValueStore: StoreBuilder[KeyValueStore[String, AggregatableObservation]] = Stores
    .keyValueStoreBuilder(Stores.persistentKeyValueStore("lane-aggregator-state-store"),
      CustomObjectSerdes.StringSerde,
      CustomObjectSerdes.AggregatableObservationSerde
    )

  val statefulStages = new StatefulStages(settings)

  val expectedOutput: Seq[AggregatableObservation] = TestObservations.observationsAfterAggregationStage.flatten
    .sortBy { f: AggregatableObservation => (f.measurementId, f.publishTimestamp) }

  "aggregation stage" should " produce correct output" in {

    builder.addStateStore(persistentKeyValueStore)
    val inputStream = builder.stream("input-topic")(Consumed.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))

    statefulStages.aggregationAfterJoinStage(inputStream)
      .selectKey { case obs: (Windowed[String], AggregatableObservation) => obs._2.measurementId }
      .to("output-topic")(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))

    val topology = builder.build()
    val topologyTestDriver = new TopologyTestDriver(topology, props)

    val inputTopic = topologyTestDriver.createInputTopic[String, AggregatableObservation]("input-topic", new StringSerializer, new AggregatableObservationSerializer)
    val outputTopic = topologyTestDriver.createOutputTopic[String, AggregatableObservation]("output-topic", new StringDeserializer, new AggregatableObservationDeserializer)

    TestObservations.observationsAfterJoinStage.foreach { next =>
      next.groupBy(_._2._1.measurementId).foreach { observationsOfMeasurementId =>
        observationsOfMeasurementId._2.foreach{ obs =>
          new AggregatableObservation(obs._2._1, obs._2._2)
          inputTopic.pipeInput(obs._1, new AggregatableObservation(obs._2._1, obs._2._2))
        }
      }
    }

    val myOutput = outputTopic.readValuesToList().asScala
      .sortBy { f: AggregatableObservation => (f.measurementId, f.publishTimestamp) }

    myOutput.foreach(println)
    myOutput should contain allElementsOf expectedOutput
    topologyTestDriver.close()
  }
}