package flink.benchmark.stages

import common.benchmark.{AggregatableObservation, RelativeChangeObservation}
import common.utils.TestObservations
import flink.benchmark.BenchmarkSettingsForFlink
import flink.benchmark.testutils.RelativeChangeCollectSink
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class RelativeChangeStageTest extends FlatSpec with Matchers with BeforeAndAfter {
  val settings = new BenchmarkSettingsForFlink

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

  "relative change stage" should " produce correct output" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source1 = env.addSource(new SourceFunction[AggregatableObservation]() {
      override def run(ctx: SourceContext[AggregatableObservation]) {
        TestObservations.observationsInputRelativeChangeStage.foreach { next =>
          next.distinct.foreach { obs =>
            ctx.collectWithTimestamp(obs, obs.publishTimestamp)
            ctx.emitWatermark(new Watermark(obs.publishTimestamp-50))
        }
        }
        ctx.close()
      }
      override def cancel(): Unit = ()
    })

    val statefulStages = new StatefulStages(settings)
    statefulStages.slidingWindowAfterAggregationStage(source1)
      .addSink(new RelativeChangeCollectSink())

    env.execute("relative-change-stage-test")

    val expectedResult = TestObservations.observationsAfterRelativeChangeStage
      .flatten.sortBy { f: RelativeChangeObservation => (f.measurementId, f.aggregatedObservation.publishTimestamp) }

    RelativeChangeCollectSink.values should contain allElementsOf(expectedResult)
  }
}

