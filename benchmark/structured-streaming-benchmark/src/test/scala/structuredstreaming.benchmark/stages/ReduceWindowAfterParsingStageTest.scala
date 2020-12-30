package structuredstreaming.benchmark.stages

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, StreamingSuiteBase}
import common.benchmark.AggregatableFlowObservation
import common.utils.TestObservations
import org.apache.spark.sql.functions.{col, date_trunc, udf}
import org.apache.spark.sql.types.TimestampType
import org.scalatest.FunSuite
import structuredstreaming.benchmark.BenchmarkSettingsForStructuredStreaming

class ReduceWindowAfterParsingStageTest extends FunSuite with DataFrameSuiteBase with StreamingSuiteBase with RDDComparisons {

  // Due to net.jpountz.lz4 version incompatibility we switch to snappy for the tests
  System.setProperty("spark.io.compression.codec", "snappy")

  import spark.implicits._

  val overrides: Map[String, Any] = Map("general.last.stage" -> "5",
    "general.window.after.parsing.window.duration" -> 300000,
    "general.window.after.parsing.slide.duration" -> 60000)
  private val settings = new BenchmarkSettingsForStructuredStreaming(overrides)

  test("aggregate flow events per measurement ID") {
    // Aggregate over lanes per measurement ID
    val analytics = new StatefulStages(spark, settings)

    val getTimestamp = udf((flowTimestamp: Long) => new Timestamp(flowTimestamp))
    val inputwindowAfterParsingStage = sc.parallelize(TestObservations.flowObservationsAfterParsingStage.flatten)
      .map(el => new AggregatableFlowObservation(el._2))
      .toDF()
      .withColumn("publishTimestamp", getTimestamp(col("publishTimestamp")))
      .withColumn("timestamp", date_trunc("second", col("publishTimestamp")))
    inputwindowAfterParsingStage.show(100)

    val expectedOutputWindowAfterParsingStage = TestObservations.outputWindowAfterParsingStage.flatten.map(_._2)
      .toDF()
      .withColumn("publishTimestamp", (col("publishTimestamp") / 1000).cast(TimestampType))
      .drop("jobProfile")
      .orderBy("publishTimestamp", "measurementId", "laneCount")
    expectedOutputWindowAfterParsingStage.show()

    val outputWindowAfterParsingStage = analytics.reduceWindowAfterParsingStage(inputwindowAfterParsingStage)
      .select("measurementId", "laneCount", "publishTimestamp", "latitude", "longitude", "accumulatedFlow",
        "period", "flowAccuracy", "numLanes")
      .orderBy("publishTimestamp", "measurementId", "laneCount")
    outputWindowAfterParsingStage.show()

    assertRDDEquals(expectedOutputWindowAfterParsingStage.rdd, outputWindowAfterParsingStage.rdd)
  }
}