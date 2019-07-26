
package spark.benchmark.phases

import com.holdenkarau.spark.testing.{RDDComparisons, StreamingSuiteBase}
import common.benchmark.{AggregatableObservation, RelativeChangeObservation}
import common.utils.TestObservations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spark.benchmark.stages.AnalyticsStages
import spark.benchmark.{BenchmarkSettingsForSpark, SparkTrafficAnalyzer}

import scala.collection.immutable

/**
 * Test relative change calculation phase
 *
 * - Uses test observations of common-benchmark/src/test/scala/common/utils/TestObservations.scala
  * */
class RelativeChangeStageTest extends FunSuite with StreamingSuiteBase {

  // Due to net.jpountz.lz4 version incompatibility we switch to snappy for the tests
  System.setProperty("spark.io.compression.codec", "snappy")

  private val settings = new BenchmarkSettingsForSpark()
  val testDataAfterAggregationPhase: Seq[AggregatableObservation] = TestObservations.observationsInputRelativeChangePhase.flatten

  // For Spark only complete results will be returned, otherwise there will be too much output for the single burst workload
  // This is due to the lach of event time processing for Spark
  val expectedResultOfRelativeChangePhase: Seq[RelativeChangeObservation] = TestObservations.observationsAfterRelativeChangePhase.flatten
      .filter{obs: RelativeChangeObservation => obs.shortDiff.isDefined & obs.longDiff.isDefined}

  // Execute test
  test("compute relative change per measurement ID"){
    // Aggregate over lanes per measurement ID
    val analyticsStages = new AnalyticsStages(settings)
    testOperation(Seq(testDataAfterAggregationPhase), analyticsStages.relativeChangesStage _, Seq(expectedResultOfRelativeChangePhase))
  }
}

