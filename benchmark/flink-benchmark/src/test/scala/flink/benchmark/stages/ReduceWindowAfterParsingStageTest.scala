package flink.benchmark.stages

import common.benchmark.{AggregatableFlowObservation, FlowObservation}
import common.utils.TestObservations
import flink.benchmark.BenchmarkSettingsForFlink
import flink.benchmark.testutils.AggregatableFlowCollectSink
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._


class ReduceWindowAfterParsingStageTest extends FlatSpec with Matchers with BeforeAndAfter {
  val overrides: Map[String, Any] = Map("general.last.stage" -> "5",
    "general.window.after.parsing.window.duration" -> 300000,
    "general.window.after.parsing.slide.duration" -> 60000)
  val settings = new BenchmarkSettingsForFlink(overrides)

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build())

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  "window after parsing stage " should " have produce correct output " in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source1 = env.addSource(new SourceFunction[FlowObservation]() {
      override def run(ctx: SourceContext[FlowObservation]) {
        TestObservations.flowObservationsAfterParsingStage.foreach { next =>
          next.foreach { case (_: String, obs: FlowObservation) =>
            ctx.collectWithTimestamp(obs, obs.publishTimestamp)
            ctx.emitWatermark(new Watermark(obs.publishTimestamp-50))
          }
        }
        ctx.close()
      }
      override def cancel(): Unit = ()
    })

    val statefulStages = new StatefulStages(settings)
    val aggregatedStream = statefulStages.reduceWindowAfterParsingStage(source1)
    aggregatedStream.addSink(new AggregatableFlowCollectSink())

    env.execute("window-after-parsing-stage-test")

    val expectedResult = TestObservations.outputWindowAfterParsingStage.flatten.map(_._2)
      .sortBy { f: AggregatableFlowObservation => (f.measurementId, f.publishTimestamp) }

    val collectedValues = AggregatableFlowCollectSink.values.asScala
      .sortBy { f: AggregatableFlowObservation => (f.measurementId, f.publishTimestamp) }

    collectedValues should contain allElementsOf(expectedResult)
  }
}
