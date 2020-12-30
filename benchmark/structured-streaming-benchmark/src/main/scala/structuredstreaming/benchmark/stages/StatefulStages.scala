package structuredstreaming.benchmark.stages

import java.sql.Timestamp

import common.benchmark._
import common.benchmark.stages.StatefulStagesTemplate
import common.config.LastStage.UNTIL_TUMBLING_WINDOW
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{max, window, _}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.TimestampType
import structuredstreaming.benchmark.BenchmarkSettingsForStructuredStreaming

import scala.util.Try

/**
  * Contains all methods required in the aggregation and windowing phase.
  *
  * @param sparkSession
  * @param settings settings for the job
  */
class StatefulStages(sparkSession: SparkSession, settings: BenchmarkSettingsForStructuredStreaming)
  extends Serializable with StatefulStagesTemplate {

  import sparkSession.implicits._

  /**
   * Joins the flow and speed streams
   *
   * @param parsedFlowStream  Dataframe with flow events
   * @param parsedSpeedStream Dataframe with speed events
   * @return Dataframe with joined events of both streams
   */
  def joinStage(parsedFlowStream: DataFrame, parsedSpeedStream: DataFrame): DataFrame = {
    val flowStreamWithWatermark = parsedFlowStream
      .withColumn("window", window(col("timestamp"),
        settings.general.publishIntervalMillis + " milliseconds",
        settings.general.publishIntervalMillis + " milliseconds"))
      .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")

    val speedStreamWithWatermark = parsedSpeedStream
      .withColumn("window", window(col("timestamp"),
        settings.general.publishIntervalMillis + " milliseconds",
        settings.general.publishIntervalMillis + " milliseconds"))
      .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")

    val latestTimestampUDF = udf((flowTimestamp: Timestamp, speedTimestamp: Timestamp) =>
      if (flowTimestamp.before(speedTimestamp)) speedTimestamp else flowTimestamp)

    val joinedSpeedAndFlowStreams = flowStreamWithWatermark
      .join(speedStreamWithWatermark, Seq("measurementId", "internalId", "timestamp", "window"))
      .select(
        $"measurementId",
        $"internalId".as("lanes"),
        $"timestamp",
        $"flowObservation.lat".as("latitude"),
        $"flowObservation.long".as("longitude"),
        $"flowObservation.flow".as("accumulatedFlow"),
        $"flowObservation.period".as("period"),
        $"flowObservation.accuracy".as("flowAccuracy"),
        $"speedObservation.speed".as("averageSpeed"),
        $"speedObservation.accuracy".as("speedAccuracy"),
        $"flowObservation.num_lanes".as("numLanes"),
        latestTimestampUDF($"flowPublishTimestamp", $"speedPublishTimestamp").as("publishTimestamp")
      )

    joinedSpeedAndFlowStreams
  }

  /**
    * Incrementally aggregates lanes belonging to same measurement ID and timestamp.
    *
    * - Takes the average of the speed over all the lanes.
    * - Sums up the flow over all the lanes.
    * - Appends the maps and lists of timestamps and lanes.
    *
    * @param parsedAndJoinedStream Dataframe with joined data of speed and flow events
    * @return Dataframe containing the aggregates over each lane
    */

  def aggregationAfterJoinStage(parsedAndJoinedStream: DataFrame): DataFrame = {
    val aggregatedStream = parsedAndJoinedStream
        .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")
        .groupBy(
          window($"timestamp", settings.general.publishIntervalMillis + " milliseconds", settings.general.publishIntervalMillis + " milliseconds"),
          $"measurementId", $"timestamp", $"latitude", $"longitude", $"period", $"flowAccuracy", $"speedAccuracy", $"numLanes"
        )
        .agg(
          collect_set($"lanes").as("lanes"),
          max("publishTimestamp").as("publishTimestamp"),
          sum($"accumulatedFlow").as("accumulatedFlow"),
          avg($"averageSpeed").as("averageSpeed")
        ).drop("window")

    aggregatedStream
  }

  /**
   * Incrementally aggregates lanes belonging to same measurement ID and timestamp.
   *
   * - Takes the average of the speed over all the lanes.
   * - Sums up the flow over all the lanes.
   * - Appends the maps and lists of timestamps and lanes.
   *
   * @param parsedAndJoinedStream Dataframe with joined data of speed and flow events
   * @return Dataframe containing the aggregates over each lane
   */

  def lowLevelAggregationAfterJoinStage(parsedAndJoinedStream: DataFrame): DataFrame = {
    val aggregatedStream = parsedAndJoinedStream
        .withColumn("lanes", array(col("lanes")))
        .as[AggregatableObservationWithTimestamp]
        .withWatermark("publishTimestamp", settings.specific.watermarkMillis + " milliseconds")
        .groupByKey(event => (event.measurementId, event.publishTimestamp))
        .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout())(aggregateFunction).toDF()

    aggregatedStream
  }
  /**
   * Non-incrementally aggregates/computes the relative change of the current observation compared to previous ones.
   *
   * - Takes the average of the speed over all the lanes.
   * - Sums up the flow over all the lanes.
   * - Appends the maps and lists of timestamps and lanes.
   *
   * @param parsedAndJoinedStream Dataframe with joined data of speed and flow events
   * @return Dataframe containing the aggregates over each lane
   */
  def lowLevelAggregationAndSlidingWindowStage(parsedAndJoinedStream: DataFrame): Dataset[RelativeChangeObservationWithTimestamp] = {
    parsedAndJoinedStream
      .withColumn("lanes", array(col("lanes")))
      .as[AggregatableObservationWithTimestamp]
      .withWatermark("publishTimestamp", settings.specific.watermarkMillis + " milliseconds")
      .groupByKey(event => (event.measurementId, event.publishTimestamp))
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(aggregateFunction)
      .withWatermark("publishTimestamp", settings.specific.watermarkMillis + " milliseconds")
      .groupByKey(_.measurementId)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(computeRelativeChange)
  }


  /**
   * Incrementally computes a tumbling or sliding window on the flow stream
   * When the slide and tumble interval are equal, this is a tumbling window.
   * When the slide and tumble interval are different, this is a sliding window.
   *
   * @param parsedFlowStream parsed stream of flow data
   * @return aggregated data on a slide and tumble interval
   */
  def reduceWindowAfterParsingStage(parsedFlowStream: DataFrame): DataFrame = {

    val aggregatedFlowStream =
      parsedFlowStream
        .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")
        .groupBy(
          window($"timestamp", settings.general.windowDurationMsOfWindowAfterParse + " milliseconds",
            settings.general.slideDurationMsOfWindowAfterParse + " milliseconds"),
          $"measurementId",
        )
        .agg(
          first($"latitude").as("latitude"),
          first($"longitude").as("longitude"),
          first($"period").as("period"),
          first($"flowAccuracy").as("flowAccuracy"),
          first($"numLanes").as("numLanes"),
          sum($"laneCount").as("laneCount"),
          max("timestamp").as("timestamp"),
          max("publishTimestamp").as("publishTimestamp"),
          sum($"accumulatedFlow").as("accumulatedFlow"),
        ).drop("window")

    aggregatedFlowStream
  }

  /**
   * Non-incrementally computes a tumbling or sliding window on the flow stream
   * When the slide and tumble interval are equal, this is a tumbling window.
   * When the slide and tumble interval are different, this is a sliding window.
   *
   * @param parsedFlowStream parsed stream of flow data
   * @return aggregated data on a slide and tumble interval
   */
  def nonIncrementalWindowAfterParsingStage(parsedFlowStream: DataFrame): DataFrame = {

    val aggregatedFlowStream =
      parsedFlowStream
        .as[AggregatableFlowObservationWithTimestamp]
        .withWatermark("timestamp", settings.specific.watermarkMillis + " milliseconds")
        .groupByKey(_.measurementId)
        .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(computeNonIncrementalAggregation)
        .toDF()

    aggregatedFlowStream
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
        state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + 1000)
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
      val maxTime = observations.maxBy(_.publishTimestamp.getTime).publishTimestamp

      val outputObservations = observations.map { obs =>
        val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservationsForStructuredStreaming(newest = obs, oldReference =
          DataScienceMaths.lookbackInTimeForStructuredStreaming(settings.general.shortTermBatchesLookback, oldObservationsList, obs.publishTimestamp))
        val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservationsForStructuredStreaming(newest = obs, oldReference =
          DataScienceMaths.lookbackInTimeForStructuredStreaming(settings.general.longTermBatchesLookback, oldObservationsList, obs.publishTimestamp))
        RelativeChangeObservationWithTimestamp(obs.measurementId, obs.publishTimestamp, obs, shortTermChange, longTermChange)
      }

      val cleanedObservations = (oldObservationsList ++ observations).filter(obs => obs.publishTimestamp.getTime > maxTime.getTime - settings.general.longWindowLengthMillis)

      state.update(cleanedObservations)
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + 1000)
      outputObservations.toIterator
    }
  }

  private def computeNonIncrementalAggregation(key: String, events: Iterator[AggregatableFlowObservationWithTimestamp], state: GroupState[List[AggregatableFlowObservationWithTimestamp]]): Iterator[AggregatableFlowObservationWithTimestamp] = {
    if (state.hasTimedOut) {
      state.remove()
      Iterator.empty
    } else {
      if (state.exists){
        // current time of the stream
        // current time = end time of previous batch + slide interval
        val currentTime = state.getCurrentWatermarkMs() + settings.general.slideDurationMsOfWindowAfterParse

        // part of old state that is still valid
        val oldValidState = state.get.filter(_.timestamp.getTime > currentTime - settings.general.windowDurationMsOfWindowAfterParse)

        // events of this window
        val newEvents = events.toList.filter(_.timestamp.getTime > currentTime - settings.general.windowDurationMsOfWindowAfterParse)
        // sometimes an event of the next window creeps in because of batching, we filter these out for these results
        // but we need to keep them in the state for the next interval
        val eventsFromThisSlideInterval = newEvents.filter(_.timestamp.getTime < currentTime)
        val completeWindow = (oldValidState ++ eventsFromThisSlideInterval)

        val aggregationResult = completeWindow.reduce{
          (aggregatableFlowObservation1, aggregatableFlowObservation2) =>
            aggregatableFlowObservation1.combineObservations(aggregatableFlowObservation2)
        }

        state.update((newEvents ++ oldValidState))

        // when we haven't seen this measurement id for 5xslide_interval, clean up state
        // this should never happen in our stream
        state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + 5*settings.general.windowDurationMsOfWindowAfterParse)

        // send out result
        // trigger of pipeline is set to slide-interval so this should only happen 1x per slide interval
        Iterator(aggregationResult)
      } else {
        // no state yet so only the currently incoming events need to be processed
        // events of this window
        val completeWindow = events.toList

        val aggregationResult = completeWindow.reduce{
          (aggregatableFlowObservation1, aggregatableFlowObservation2) =>
            aggregatableFlowObservation1.combineObservations(aggregatableFlowObservation2)
        }

        state.update(completeWindow)

        // when we haven't seen this measurement id for 5xslide_interval, clean up state
        // for the first batch we don't have a watermark (=-1) so we take the maximum event time of the batch
        val maxTime = completeWindow.map(_.timestamp.getTime).max
        state.setTimeoutTimestamp(maxTime + 5*settings.general.windowDurationMsOfWindowAfterParse)

        // send out result
        // trigger of pipeline is set to slide-interval so this should only happen 1x per slide interval
        Iterator(aggregationResult)
      }
    }
  }
}
