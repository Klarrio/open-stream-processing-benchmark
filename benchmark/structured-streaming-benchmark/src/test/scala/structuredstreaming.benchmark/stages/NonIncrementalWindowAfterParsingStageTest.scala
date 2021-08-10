package structuredstreaming.benchmark.stages

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, StreamingSuiteBase}
import common.benchmark.{AggregatableFlowObservation, AggregatableFlowObservationWithTimestamp}
import common.utils.TestObservations
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, date_trunc, udf}
import org.apache.spark.sql.types.TimestampType
import org.scalatest.FunSuite
import structuredstreaming.benchmark.BenchmarkSettingsForStructuredStreaming
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.collection.JavaConverters._

class NonIncrementalWindowAfterParsingStageTest extends FunSuite with DataFrameSuiteBase with StreamingSuiteBase with RDDComparisons {

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
    val inputwindowAfterParsingStage: Seq[Array[AggregatableFlowObservationWithTimestamp]] =
      TestObservations.flowObservationsAfterParsingStage.map { elements =>
        val rdd = sc.parallelize(elements)
        rdd.map(el => new AggregatableFlowObservation(el._2))
          .toDF()
          .withColumn("publishTimestamp", getTimestamp(col("publishTimestamp")))
          .withColumn("timestamp", date_trunc("second", col("publishTimestamp")))
          .as[AggregatableFlowObservationWithTimestamp]
          .collect()
      }

    val events = MemoryStream[AggregatableFlowObservationWithTimestamp]
    val sessions = events.toDF()
    val outputWindowAfterParsingStage = analytics.nonIncrementalWindowAfterParsingStage(sessions.toDF())
      .select("measurementId", "laneCount", "publishTimestamp", "latitude", "longitude", "accumulatedFlow",
        "period", "flowAccuracy", "numLanes")

    val query = outputWindowAfterParsingStage.writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Append())
      .start()


    inputwindowAfterParsingStage.foreach { elements =>
      val currentOffset = events.addData(elements).asInstanceOf[LongOffset]
//      events.commit(currentOffset)
      query.processAllAvailable()
    }
    query.processAllAvailable()


    val myOutputList: List[Row] = spark.sql("select * from Output")
      .collectAsList().asScala.toList

    val expectedOutputWindowAfterParsingStage = TestObservations.outputWindowAfterParsingStage.flatten.map(_._2)
      .toDF()
      .withColumn("publishTimestamp", (col("publishTimestamp") / 1000).cast(TimestampType))
      .drop("jobProfile")
      .rdd.collect().toList

    assert(myOutputList, expectedOutputWindowAfterParsingStage)

  }
}