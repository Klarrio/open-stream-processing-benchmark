package structuredstreaming.benchmark.stages

import java.sql.Timestamp

import common.benchmark._
import common.benchmark.stages.AnalyticsStagesTemplate
import common.config.LastStage.UNTIL_TUMBLING_WINDOW
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{max, window, _}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.TimestampType
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
    val timeUDF = udf((time: Timestamp) => time.getTime)

    val aggregatedStream = if (settings.specific.useCustomTumblingWindow) {
      parsedAndJoinedStream
        .withColumn("lanes", array(col("lanes")))
        .withColumn("publishTimestamp", timeUDF($"publishTimestamp"))
        .as[AggregatableObservationWithTimestamp]
        .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")
        .groupByKey(event => (event.measurementId, event.timestamp))
        .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout())(aggregateFunction).toDF()
    } else {
      parsedAndJoinedStream
        .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")
        .groupBy(
          window($"timestamp", settings.general.publishIntervalMillis + " milliseconds", settings.general.publishIntervalMillis + " milliseconds"),
          $"measurementId", $"timestamp", $"latitude", $"longitude", $"period", $"flowAccuracy", $"speedAccuracy", $"numLanes"
        )
        .agg(
          collect_set($"lanes").as("lanes"),
          sum($"accumulatedFlow").as("accumulatedFlow"),
          avg($"averageSpeed").as("averageSpeed"),

          max("publishTimestamp").as("publishTimestamp"),
          max("ingestTimestamp").as("ingestTimestamp")
        ).drop("window")
        .withColumn("publishTimestamp", timeUDF($"publishTimestamp"))
    }

    aggregatedStream
  }

  def customAggregationAndRelativeChangeStage(parsedAndJoinedStream: DataFrame): Dataset[RelativeChangeObservationWithTimestamp] = {
    val timeUDF = udf((time: Timestamp) => time.getTime)

    parsedAndJoinedStream
      .withColumn("lanes", array(col("lanes")))
      .withColumn("publishTimestamp", timeUDF($"publishTimestamp"))
      .as[AggregatableObservationWithTimestamp]
      .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")
      .groupByKey(event => (event.measurementId, event.timestamp))
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout())(aggregateFunction)
      .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")
      .groupByKey(_.measurementId)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout())(computeRelativeChange)

  }

  private def aggregateFunction(key: (String, Timestamp), events: Iterator[AggregatableObservationWithTimestamp], state: GroupState[AggregatableObservationWithTimestamp]): Iterator[AggregatableObservationWithTimestamp] = {
    if (state.hasTimedOut) {
      state.remove()
      Iterator.empty
    } else {
      val currentEvent = if (state.exists) {
        events.foldLeft(state.get) { (e1, e2) => e1.combineObservations(e2) }
      } else events.reduce { (e1, e2) => e1.combineObservations(e2) }

      if (currentEvent.lanes.length == currentEvent.numLanes) {
        state.remove()
        Iterator(currentEvent)
      } else {
        state.update(currentEvent)

        Iterator.empty
      }
    }
  }

  private def computeRelativeChange(key: String, events: Iterator[AggregatableObservationWithTimestamp], state: GroupState[List[AggregatableObservationWithTimestamp]]): Iterator[RelativeChangeObservationWithTimestamp] = {
    if (state.hasTimedOut) {
      state.remove()
      Iterator.empty
    } else {
      val observations = events.toVector
      val oldObservationsList = state.getOption.getOrElse(List())
      val maxTime = observations.maxBy(_.timestamp.getTime).timestamp

      val outputObservations = observations.map { obs =>
        val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservationsForStructuredStreaming(newest = obs, oldReference =
          DataScienceMaths.lookbackInTimeForStructuredStreaming(settings.general.shortTermBatchesLookback, oldObservationsList, obs.timestamp))
        val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservationsForStructuredStreaming(newest = obs, oldReference =
          DataScienceMaths.lookbackInTimeForStructuredStreaming(settings.general.longTermBatchesLookback, oldObservationsList, obs.timestamp))
        RelativeChangeObservationWithTimestamp(obs.measurementId, obs.publishTimestamp, obs, shortTermChange, longTermChange)
      }

      val cleanedObservations = (oldObservationsList ++ observations).filter(obs => obs.timestamp.getTime > maxTime.getTime - settings.general.longWindowLengthMillis)

      state.update(cleanedObservations)
      outputObservations.toIterator
    }
  }
}
