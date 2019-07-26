package flink.benchmark.stages

import java.util.Properties

import common.benchmark._
import common.benchmark.stages.AnalyticsStagesTemplate
import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Contains all methods required in the aggregation and windowing phase.
  *
  * @param settings Flink configuration properties
  */
class AnalyticsStages(
  settings: BenchmarkSettingsForFlink,
  kafkaProperties: Properties
) extends Serializable with AnalyticsStagesTemplate {

  /**
    * Aggregates lanes belonging to same measurement ID and timestamp.
    *
    * - Takes the average of the speed over all the lanes.
    * - Sums up the flow over all the lanes.
    * - Appends the maps and lists of timestamps and lanes.
    *
    * @param parsedAndJoinedStream [[FlowObservation]] and [[SpeedObservation]] for one lane for a measurement ID
    * @return [[DataStream]] of [[AggregatableObservation]]
    */
  def aggregationStage(parsedAndJoinedStream: DataStream[AggregatableObservation], test: Boolean = false): DataStream[AggregatableObservation] = {
    // summing up over all the lanes of a measurement point
    val aggregatedStream = parsedAndJoinedStream
      .keyingBy(obs => (obs.measurementId, obs.timestamp))
      .window(TumblingEventTimeWindows.of(Time.milliseconds(settings.general.publishIntervalMillis)))
      .reduceWith {
        case (aggregatedObservation1, aggregatedObservation2) =>
          aggregatedObservation1.combineObservations(aggregatedObservation2)
      }

    aggregatedStream
  }

  /**
    * Computes the relative change of a certain measurement ID over time.
    *
    * Short and long term relative change is computed for flow and speed
    * [obs(t) - obs(t-n)]/obs(t-n)
    *
    * @param aggregatedStream [[DataStream]] of [[AggregatableObservation]]
    * @return [[DataStream]] of the [[RelativeChangeObservation]]
    */
  def relativeChangeStage(aggregatedStream: DataStream[AggregatableObservation], test: Boolean = false): DataStream[RelativeChangeObservation] = {
    val relativeChangeStream = aggregatedStream
      .keyBy {_.measurementId}
      .window(SlidingEventTimeWindows.of(Time.milliseconds(settings.general.longWindowLengthMillis), Time.milliseconds(settings.general.windowSlideIntervalMillis)))
      .apply(calculateLongAndShortRelativeChange)

    relativeChangeStream
  }

  /**
    * Calculates the relative change of flow and speed for the current observation
    *
    * Is applied to a iterable that contains all [[AggregatableObservation]] of the [[TimeWindow]]
    *
    * @return [[RelativeChangeObservation]] of the current observation
    */
  def calculateLongAndShortRelativeChange: (String, TimeWindow, Iterable[AggregatableObservation], Collector[RelativeChangeObservation]) => Unit =
    (_, window, in, out) => {
      val observations = in.toVector
      val obs = observations.maxBy {_.timestamp}

      // Only return output for the keys that were in the last slide part of the window
      // So where the publishtimestamp of the observation is between the beginning of this slide and the end of the window
      if (obs.publishTimestamp >= window.getEnd - settings.general.windowSlideIntervalMillis) {
        out.collect({
          val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
            DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, observations, obs.timestamp))
          val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
            DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, observations, obs.timestamp))
          RelativeChangeObservation(obs.measurementId, obs.publishTimestamp, obs, shortTermChange, longTermChange)
        })
      }
    }
}
