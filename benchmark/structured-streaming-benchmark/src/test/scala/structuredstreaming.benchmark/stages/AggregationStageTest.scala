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
    val analytics = new AnalyticsStages(spark, settings)

    val inputAggregationStage = sc.parallelize(TestObservations.observationsAfterJoinPhase.flatten)
      .map { case (key: String, (flowObservation: FlowObservation, speedObservation: SpeedObservation)) =>
        (flowObservation.measurementId, flowObservation.internalId, new Timestamp(flowObservation.timestamp), flowObservation.latitude,
          flowObservation.longitude, flowObservation.flow, flowObservation.period, flowObservation.accuracy,
          speedObservation.speed, speedObservation.accuracy, flowObservation.numLanes,
          new Timestamp(Math.max(flowObservation.publishTimestamp, speedObservation.publishTimestamp)),
          new Timestamp(Math.max(flowObservation.ingestTimestamp, speedObservation.ingestTimestamp))
        )
      }.toDF("measurementId", "lanes", "timestamp", "latitude", "longitude", "accumulatedFlow", "period",
      "flowAccuracy", "averageSpeed", "speedAccuracy", "numLanes", "publishTimestamp", "ingestTimestamp")

    val expectedOutputAggregationStage = TestObservations.observationsAfterAggregationPhase.flatten
      .toDF()
      .withColumn("publishTimestamp", (col("publishTimestamp") / 1000).cast(TimestampType))
      .withColumn("ingestTimestamp", (col("ingestTimestamp") / 1000).cast(TimestampType))
      .withColumn("timestamp", (col("timestamp") / 1000).cast(TimestampType))
      .orderBy("timestamp", "measurementId", "lanes")
      .withColumn("lanes", sort_array(col("lanes")))
    expectedOutputAggregationStage.show()

    val realOutputAggregationStage = analytics.aggregationStage(inputAggregationStage)
      .select("measurementId", "lanes", "timestamp", "latitude", "longitude", "accumulatedFlow",
        "period", "flowAccuracy", "averageSpeed", "speedAccuracy", "numLanes", "publishTimestamp", "ingestTimestamp")
      .orderBy("timestamp", "measurementId", "lanes")
      .withColumn("lanes", sort_array(col("lanes")))
    realOutputAggregationStage.show()

    assertRDDEquals(expectedOutputAggregationStage.rdd, realOutputAggregationStage.rdd)
  }
}

