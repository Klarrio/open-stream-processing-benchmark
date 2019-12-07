package structuredstreaming.benchmark.stages

import common.benchmark.stages.AnalyticsStagesTemplate
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{max, window, _}
import structuredstreaming.benchmark.BenchmarkSettingsForStructuredStreaming

/**
  * Contains all methods required in the aggregation and windowing phase.
  *
  * @param sparkSession
  * @param settings settings for the job
  */
class AnalyticsStages(sparkSession: SparkSession, settings: BenchmarkSettingsForStructuredStreaming)
  extends Serializable with AnalyticsStagesTemplate {

  import sparkSession.implicits._

  /**
    * Aggregates lanes belonging to same measurement ID and timestamp.
    *
    * - Takes the average of the speed over all the lanes.
    * - Sums up the flow over all the lanes.
    * - Appends the maps and lists of timestamps and lanes.
    *
    * @param parsedAndJoinedStream Dataframe with joined data of speed and flow events
    * @return Dataframe containing the aggregates over each lane
    */

  def aggregationStage(parsedAndJoinedStream: DataFrame): DataFrame = {
    val aggregatedStream = parsedAndJoinedStream
      .withWatermark("publishTimestamp", settings.specific.watermarkMillis + " milliseconds")
      .groupBy(
        window($"publishTimestamp", settings.general.publishIntervalMillis + " milliseconds", settings.general.publishIntervalMillis + " milliseconds"),
        $"measurementId", $"timestamp", $"latitude", $"longitude", $"period", $"flowAccuracy", $"speedAccuracy", $"numLanes"
      )
      .agg(
        collect_set($"lanes").as("lanes"),
        sum($"accumulatedFlow").as("accumulatedFlow"),
        avg($"averageSpeed").as("averageSpeed"),

        max("publishTimestamp").as("publishTimestamp"),
        max("ingestTimestamp").as("ingestTimestamp")
      ).drop("window")

    aggregatedStream
  }
}
