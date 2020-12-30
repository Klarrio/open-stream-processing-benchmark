package flink.benchmark.stages

import common.benchmark.{AggregatableObservation, FlowObservation, SpeedObservation}
import common.utils.TestObservations
import flink.benchmark.BenchmarkSettingsForFlink
import flink.benchmark.testutils.AggregatableObservationCollectSink
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AggregationStageTest extends FlatSpec with Matchers with BeforeAndAfter {
  val settings = new BenchmarkSettingsForFlink()

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build())
  
  val statefulStages = new StatefulStages(settings)

  val expectedResult: Seq[AggregatableObservation] = TestObservations.observationsAfterAggregationStage.flatten
    .sortBy { f: AggregatableObservation => (f.measurementId, f.publishTimestamp) }

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }

  "aggregation stage" should " produce correct output" in  {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source1 = env.addSource(new SourceFunction[(FlowObservation, SpeedObservation)]() {
      override def run(ctx: SourceContext[(FlowObservation, SpeedObservation)]) {
        TestObservations.observationsAfterJoinStage.foreach { next =>
          next.foreach { obs =>
            ctx.collectWithTimestamp(obs._2, obs._2._1.publishTimestamp)
            ctx.emitWatermark(new Watermark(obs._2._1.publishTimestamp-50))
          }
        }
        ctx.close()
      }
      override def cancel(): Unit = ()
    })

    statefulStages.aggregationAfterJoinStage(source1
      .map{event: (FlowObservation, SpeedObservation) => new AggregatableObservation(event._1, event._2) })
      .addSink(new AggregatableObservationCollectSink())

    env.execute("aggregation-stage-test")

    AggregatableObservationCollectSink.values should contain allElementsOf(expectedResult)
  }
}

