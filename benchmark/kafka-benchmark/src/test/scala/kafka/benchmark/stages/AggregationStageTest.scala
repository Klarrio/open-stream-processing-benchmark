package kafka.benchmark.phases

import java.util.Properties

import common.benchmark.output.{AggregatableObservationDeserializer, AggregatableObservationSerializer, RelativeChangeDeserializer}
import common.benchmark.{AggregatableObservation, RelativeChangeObservation}
import common.utils.TestObservations
import kafka.benchmark.stages.{AnalyticsStages, CustomObjectSerdes}
import kafka.benchmark.{BenchmarkSettingsForKafkaStreams, KafkaTrafficAnalyzer}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest._

class AggregationStageTest extends FunSuite {
  val settings: BenchmarkSettingsForKafkaStreams = new BenchmarkSettingsForKafkaStreams

  val props: Properties = KafkaTrafficAnalyzer.initKafka(settings)
  val builder = new StreamsBuilder()

  val analyticsStages = new AnalyticsStages(settings)

  test("test aggregation utils") {

    val inputStream = builder.stream("input-topic")(Consumed.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))

    analyticsStages.aggregationStage(inputStream)
      .selectKey { case obs: (Windowed[String], AggregatableObservation) => obs._2.measurementId }
      .to("output-topic")(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))

    val topology = builder.build()
    val topologyTestDriver = new TopologyTestDriver(topology, props)
    val recordFactory: ConsumerRecordFactory[String, AggregatableObservation] = new ConsumerRecordFactory[String, AggregatableObservation]("input-topic", new StringSerializer, new AggregatableObservationSerializer)

    var myOutput: List[AggregatableObservation] = List()
    TestObservations.observationsAfterJoinPhase.foreach { next =>
      next.groupBy(_._2._1.measurementId).foreach { observationsOfMeasurementId =>
        observationsOfMeasurementId._2.foreach{ obs =>
          new AggregatableObservation(obs._2._1, obs._2._2)
          val cr = recordFactory.create("input-topic", obs._1, new AggregatableObservation(obs._2._1, obs._2._2), obs._2._1.timestamp)
          topologyTestDriver.pipeInput(cr)
        }
      val outputRecord = topologyTestDriver.readOutput("output-topic", new StringDeserializer, new AggregatableObservationDeserializer)
      myOutput = myOutput.:+(outputRecord.value())
      }
    }

    val expected = TestObservations.observationsAfterAggregationPhase.flatten
      .sortBy { f: AggregatableObservation => (f.measurementId, f.timestamp) }
    val myOutputList = myOutput
      .sortBy { f: AggregatableObservation => (f.measurementId, f.timestamp) }
    assert(expected == myOutputList)
    topologyTestDriver.close()
  }
}

