package flink.benchmark.stages

import java.util.Properties

import common.benchmark.{AggregatableObservation, RelativeChangeObservation}
import common.utils.TestObservations
import flink.benchmark.BenchmarkSettingsForFlink
import flink.benchmark.stages.AnalyticsStages
import org.apache.flink.contrib.streaming.DataStreamUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.JavaConverters._
import org.scalatest.FunSuite

class RelativeChangeStageTest extends FunSuite {
  val settings = new BenchmarkSettingsForFlink

  test("test windowing utils") {
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source1 = env.addSource(new SourceFunction[AggregatableObservation]() {
      override def run(ctx: SourceContext[AggregatableObservation]) {
        TestObservations.observationsInputRelativeChangePhase.foreach { next =>
          next.distinct.foreach { obs =>
            ctx.collectWithTimestamp(obs, obs.publishTimestamp)
            ctx.emitWatermark(new Watermark(obs.publishTimestamp-50))
        }
        }
        ctx.close()
      }
      override def cancel(): Unit = ()
    })

    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", settings.general.kafkaBootstrapServers)

    val aggregationAndWindowUtils = new AnalyticsStages(settings, kafkaProperties)

    val windowedStream = aggregationAndWindowUtils.relativeChangeStage(source1)
    val myOutput = DataStreamUtils.collect(windowedStream.javaStream).asScala

    env.execute("Test window utils")

    val myOutputList = myOutput.toList
      .sortBy { f: RelativeChangeObservation => (f.measurementId, f.aggregatedObservation.timestamp) }

    val expectedResult = TestObservations.observationsAfterRelativeChangePhase
      .flatten.sortBy { f: RelativeChangeObservation => (f.measurementId, f.aggregatedObservation.timestamp) }

    assert(myOutputList === expectedResult)
  }
}
