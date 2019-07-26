package flink.benchmark.stages

import java.util.Properties

import common.benchmark.{AggregatableObservation, FlowObservation, SpeedObservation}
import common.utils.TestObservations
import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.contrib.streaming.DataStreamUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.scalatest.FunSuite

import scala.collection.JavaConverters.asScalaIteratorConverter

class AggregationStageTest extends FunSuite {
  val settings = new BenchmarkSettingsForFlink()

  test("test aggregation utils") {
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source1 = env.addSource(new SourceFunction[(FlowObservation, SpeedObservation)]() {
      override def run(ctx: SourceContext[(FlowObservation, SpeedObservation)]) {
        TestObservations.observationsAfterJoinPhase.foreach { next =>
          next.foreach { obs =>
            ctx.collectWithTimestamp(obs._2, obs._2._1.timestamp)
            ctx.emitWatermark(new Watermark(obs._2._1.timestamp-50))
          }
        }
        ctx.close()
      }
      override def cancel(): Unit = ()
    })

    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", settings.general.kafkaBootstrapServers)

    val analyticsStages = new AnalyticsStages(settings, kafkaProperties)

    val aggregatedStream = analyticsStages.aggregationStage(source1
      .map{event: (FlowObservation, SpeedObservation) => new AggregatableObservation(event._1, event._2) })

    val myOutput = DataStreamUtils.collect(aggregatedStream.javaStream).asScala

    env.execute("Test aggregation utils")

    val myOutputList = myOutput.toList
      .sortBy { f: AggregatableObservation => (f.measurementId, f.timestamp) }


    val expectedResult = TestObservations.observationsAfterAggregationPhase.flatten
      .sortBy { f: AggregatableObservation => (f.measurementId, f.timestamp) }

    assert( myOutputList === expectedResult)
  }
}

