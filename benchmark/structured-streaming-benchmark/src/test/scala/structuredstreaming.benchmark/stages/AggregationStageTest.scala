package structuredstreaming.benchmark.stages

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, StreamingSuiteBase}
import common.benchmark.{FlowObservation, SpeedObservation}
import common.utils.TestObservations
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import structuredstreaming.benchmark.BenchmarkSettingsForStructuredStreaming

/**
 * Test aggregation phase
 *
 * - Uses test observations of common-benchmark/src/test/scala/common/utils/TestObservations.scala
 */
class AggregationStageTest extends FunSuite with DataFrameSuiteBase with StreamingSuiteBase with RDDComparisons {

  // Due to net.jpountz.lz4 version incompatibility we switch to snappy for the tests
  System.setProperty("spark.io.compression.codec", "snappy")

  import spark.implicits._

  private val settings = new BenchmarkSettingsForStructuredStreaming

  test("aggregate over lanes per measurement ID") {
    // Aggregate over lanes per measurement ID
    val analytics = new StatefulStages(spark, settings)

    val inputAggregationStage = sc.parallelize(TestObservations.observationsAfterJoinStage.flatten)
      .map { case (key: String, (flowObservation: FlowObservation, speedObservation: SpeedObservation)) =>
        (flowObservation.measurementId, flowObservation.internalId, new Timestamp(flowObservation.publishTimestamp), flowObservation.latitude,
          flowObservation.longitude, flowObservation.flow, flowObservation.period, flowObservation.accuracy,
          speedObservation.speed, speedObservation.accuracy, flowObservation.numLanes
        )
      }.toDF("measurementId", "lanes", "publishTimestamp", "latitude", "longitude", "accumulatedFlow", "period",
      "flowAccuracy", "averageSpeed", "speedAccuracy", "numLanes")
      .withColumn("timestamp", date_trunc("second", col("publishTimestamp")))

    val expectedOutputAggregationStage = TestObservations.observationsAfterAggregationStage.flatten
      .toDF()
      .withColumn("publishTimestamp", (col("publishTimestamp") / 1000).cast(TimestampType))
      .orderBy("publishTimestamp", "measurementId", "lanes")
      .withColumn("lanes", sort_array(col("lanes")))
      .drop("jobprofile")
    expectedOutputAggregationStage.show()

    val realOutputAggregationStage = analytics.aggregationAfterJoinStage(inputAggregationStage)
      .select("measurementId", "lanes", "publishTimestamp", "latitude", "longitude", "accumulatedFlow",
        "period", "flowAccuracy", "averageSpeed", "speedAccuracy", "numLanes")
      .orderBy("publishTimestamp", "measurementId", "lanes")
      .withColumn("lanes", sort_array(col("lanes")))
    realOutputAggregationStage.show()

    assertRDDEquals(expectedOutputAggregationStage.rdd, realOutputAggregationStage.rdd)
  }
}
