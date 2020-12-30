package spark.benchmark.stages

import com.holdenkarau.spark.testing.StreamingSuiteBase
import common.utils.TestObservations
import org.apache.spark.streaming.Duration
import org.scalatest.FunSuite
import spark.benchmark.BenchmarkSettingsForSpark

class NonIncrementalWindowAfterParsingStageTest extends FunSuite with StreamingSuiteBase {

  override def batchDuration: Duration = Duration.apply(60000)

  // Due to net.jpountz.lz4 version incompatibility we switch to snappy for the tests
  System.setProperty("spark.io.compression.codec", "snappy")

  // Execute test
  test("aggregate over sliding window per measurement ID") {
    // Setup environment
    // Initialize Apache Spark
    // Set the window to the right size for the test data
    val overrides: Map[String, Any] = Map("general.last.stage" -> "5",
      "general.window.after.parsing.window.duration" -> 300000,
      "general.window.after.parsing.slide.duration" -> 60000)
    val settings = new BenchmarkSettingsForSpark(overrides)

    // Test data which is the result of the join phase
    val parsedFlowEvents= TestObservations.flowObservationsAfterParsingStage

    // The expected result of the aggregation phase
    // Due to the processing time semantics of Spark it will not output values when the key is not in the slide window
    // These are the four values at the end of the test data. The length of the window is 300 000 ms and the slide interval is 60 000 ms.
    // So the last four values are the results that actually come after the last real input batch.
    val expectedResultOfAggregation = TestObservations.outputWindowAfterParsingStage
      .dropRight(4)

    val statefulStages = new StatefulStages(settings)

    testOperation(parsedFlowEvents, statefulStages.nonIncrementalWindowAfterParsingStage _, expectedResultOfAggregation, ordered = false)
  }
}

