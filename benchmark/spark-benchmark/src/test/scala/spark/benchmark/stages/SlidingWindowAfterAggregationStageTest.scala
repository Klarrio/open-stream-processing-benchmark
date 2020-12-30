
package spark.benchmark.stages

import com.holdenkarau.spark.testing.StreamingSuiteBase
import common.benchmark.{AggregatableObservation, RelativeChangeObservation}
import common.utils.TestObservations
import org.scalatest.FunSuite
import spark.benchmark.BenchmarkSettingsForSpark
import spark.benchmark.stages.StatefulStages

/**
 * Test relative change calculation phase
 *
 * - Uses test observations of common-benchmark/src/test/scala/common/utils/TestObservations.scala
  * */
class SlidingWindowAfterAggregationStageTest extends FunSuite with StreamingSuiteBase {

  // Due to net.jpountz.lz4 version incompatibility we switch to snappy for the tests
  System.setProperty("spark.io.compression.codec", "snappy")

  private val settings = new BenchmarkSettingsForSpark()
  val testDataAfterAggregationPhase = TestObservations.observationsInputRelativeChangeStage

  // For Spark only complete results will be returned, otherwise there will be too much output for the single burst workload
  // This is due to the lach of event time processing for Spark
  val expectedResultOfRelativeChangePhase: Seq[List[RelativeChangeObservation]] = TestObservations.observationsAfterRelativeChangeStage
      .map{ elements: List[RelativeChangeObservation] => elements.filter(el => el.shortDiff.isDefined & el.longDiff.isDefined)}

  // Execute test
  test("compute relative change per measurement ID"){
    // Aggregate over lanes per measurement ID
    val statefulStages = new StatefulStages(settings)
    testOperation(testDataAfterAggregationPhase, statefulStages.slidingWindowAfterAggregationStage _, expectedResultOfRelativeChangePhase)
  }
}

