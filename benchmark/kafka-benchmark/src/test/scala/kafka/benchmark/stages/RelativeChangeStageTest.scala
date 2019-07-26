package kafka.benchmark.phases

import java.util.Properties

import common.benchmark.output.{AggregatableObservationSerializer, RelativeChangeDeserializer}
import common.benchmark.{AggregatableObservation, RelativeChangeObservation}
import common.utils.TestObservations
import kafka.benchmark.stages.{AnalyticsStages, CustomObjectSerdes}
import kafka.benchmark.{BenchmarkSettingsForKafkaStreams, KafkaTrafficAnalyzer}
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.{Windowed, _}
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest._

class RelativeChangeStageTest extends FunSuite {

  test("test window utils") {
    val settings: BenchmarkSettingsForKafkaStreams = new BenchmarkSettingsForKafkaStreams

    val props: Properties = KafkaTrafficAnalyzer.initKafka(settings)
    val builder = new StreamsBuilder()

    val analyticsStages = new AnalyticsStages(settings)

    val inputStream = builder.stream("input-topic")(Consumed.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
      .map[Windowed[String], AggregatableObservation] {
      case mapper: (String, AggregatableObservation) =>
        (new Windowed[String](mapper._1, new TimeWindow(1000, 2000)), mapper._2)
    }
    analyticsStages.relativeChangeStage(inputStream)
      .to("output-topic")(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.RelativeChangeObservationSerde))

    val topology = builder.build()
    val topologyTestDriver = new TopologyTestDriver(topology, props)
    val recordFactory: ConsumerRecordFactory[String, AggregatableObservation] = new ConsumerRecordFactory[String, AggregatableObservation]("input-topic", new StringSerializer, new AggregatableObservationSerializer)

    var myOutput: List[RelativeChangeObservation] = List()
    TestObservations.observationsInputRelativeChangePhase.foreach { next =>
      next.distinct.foreach { obs =>
        val cr = recordFactory.create("input-topic", obs.measurementId, obs, obs.publishTimestamp)
        topologyTestDriver.pipeInput(cr)

        val outputRecord = topologyTestDriver.readOutput("output-topic", new StringDeserializer, new RelativeChangeDeserializer)
        myOutput = myOutput.:+(outputRecord.value())
      }
    }

    val expected = TestObservations.observationsAfterRelativeChangePhase.flatten
    assert(expected == myOutput)
    topologyTestDriver.close()
  }
}

