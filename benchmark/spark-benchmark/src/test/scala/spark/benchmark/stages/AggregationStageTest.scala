
package spark.benchmark.phases

import com.holdenkarau.spark.testing.StreamingSuiteBase
import common.utils.TestObservations
import org.scalatest.FunSuite
import spark.benchmark.BenchmarkSettingsForSpark
import spark.benchmark.stages.AnalyticsStages

/**
  * Test aggregation phase
  *
  * - Uses test observations of common-benchmark/src/test/scala/common/utils/TestObservations.scala
  **/
class AggregationStageTest extends FunSuite with StreamingSuiteBase {

  // Due to net.jpountz.lz4 version incompatibility we switch to snappy for the tests
  System.setProperty("spark.io.compression.codec", "snappy")

  // Execute test
  test("aggregate over lanes per measurement ID") {
    // Setup environment
    // Initialize Apache Spark
    val settings = new BenchmarkSettingsForSpark()
    // Test data which is the result of the join phase
    val testDataAfterJoinPhase = TestObservations.observationsAfterJoinPhase.flatten

    // The expected result of the aggregation phase
    val expectedResultOfAggregation = TestObservations.observationsAfterAggregationPhase.flatten

    val analyticsStages = new AnalyticsStages(settings)
    // Aggregate over lanes per measurement ID
    testOperation(Seq(testDataAfterJoinPhase), analyticsStages.aggregationStage _, Seq(expectedResultOfAggregation), ordered=false)

  }

}

