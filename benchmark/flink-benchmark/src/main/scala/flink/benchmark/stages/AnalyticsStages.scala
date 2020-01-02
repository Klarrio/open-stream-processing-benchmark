package flink.benchmark.stages

import java.util.Properties

import common.benchmark._
import common.benchmark.stages.AnalyticsStagesTemplate
import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
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
    val aggregatedStream = {
      if (settings.specific.useCustomTumblingWindow){
        parsedAndJoinedStream
          .keyingBy(obs => (obs.measurementId, obs.timestamp))
          .window(TumblingEventTimeWindows.of(Time.milliseconds(settings.general.publishIntervalMillis)))
          .trigger(new LaneCountOrEventTimeTrigger)
          .reduceWith {
            case (aggregatedObservation1, aggregatedObservation2) =>
              aggregatedObservation1.combineObservations(aggregatedObservation2)
          }
      } else {
        parsedAndJoinedStream
          .keyingBy(obs => (obs.measurementId, obs.timestamp))
          .window(TumblingEventTimeWindows.of(Time.milliseconds(settings.general.publishIntervalMillis)))
          .reduceWith {
            case (aggregatedObservation1, aggregatedObservation2) =>
              aggregatedObservation1.combineObservations(aggregatedObservation2)
          }
      }
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
    val relativeChangeStream = if (settings.specific.useCustomSlidingWindow){
      aggregatedStream
        .keyBy {_.measurementId}
        .flatMap(new ComputeRelativeChange(settings))
    } else {
      aggregatedStream
        .keyBy {_.measurementId}
        .window(SlidingEventTimeWindows.of(Time.milliseconds(settings.general.longWindowLengthMillis), Time.milliseconds(settings.general.windowSlideIntervalMillis)))
        .apply(calculateLongAndShortRelativeChange)
    }

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


/**
  * Used to reduce state in the LaneCountAndEventTimeTrigger
  */
class Sum extends ReduceFunction[Long] {
  override def reduce(value1: Long, value2: Long): Long = value1 + value2
}

/**
  * Used to trigger computation when all lanes of a road have been processed or when event time has passed the watermark
  * Based on Flink implementations of EventTimeTrigger and CountTrigger
  */
class LaneCountOrEventTimeTrigger[W <: Window] extends Trigger[AggregatableObservation, W] {
  val stateDesc: ReducingStateDescriptor[Long] = new ReducingStateDescriptor[Long]("count", new Sum(), createTypeInformation[Long])

  override def onElement(element: AggregatableObservation, timestamp: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = {
    val count = ctx.getPartitionedState(stateDesc)
    count.add(1L)
    // When all lanes of the road have been processed then fire
    if (count.get >= element.numLanes) {
      count.clear()
      TriggerResult.FIRE_AND_PURGE
    } else if (window.maxTimestamp <= ctx.getCurrentWatermark) { // if the watermark is already past the window fire immediately
      count.clear()
      TriggerResult.FIRE_AND_PURGE
    }
    else {
      ctx.registerEventTimeTimer(window.maxTimestamp)
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = {
    if(time == window.maxTimestamp()) {
      TriggerResult.PURGE
    } else TriggerResult.CONTINUE
  }


  override def onProcessingTime(time: Long, window: W, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE


  override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(stateDesc).clear()
    ctx.deleteEventTimeTimer(window.maxTimestamp())
    TriggerResult.PURGE
  }

  override def canMerge = true


  override def onMerge(window: W, ctx: Trigger.OnMergeContext): Unit = {
    ctx.mergePartitionedState(stateDesc)
    if (window.maxTimestamp() > ctx.getCurrentWatermark)
      ctx.registerEventTimeTimer(window.maxTimestamp())
  }

  override def toString: String = "LaneCountAndEventTimeTrigger"
}


class ComputeRelativeChange(settings: BenchmarkSettingsForFlink) extends RichFlatMapFunction[AggregatableObservation, RelativeChangeObservation]{

  private var pastObservations: ListState[AggregatableObservation] = _

  private var maxTimestamp: ValueState[Long] = _


  override def flatMap(value: AggregatableObservation, out: Collector[RelativeChangeObservation]): Unit = {
    // process the observation if it is not less than the low watermark
    if(value.publishTimestamp > maxTimestamp.value() - settings.specific.maxOutOfOrderness) {

      if (value.publishTimestamp > maxTimestamp.value()) maxTimestamp.update(value.publishTimestamp)

      val oldObservationsSeq: Seq[AggregatableObservation] = getCleanListOfOldObservations
      val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = value, oldReference =
        DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, oldObservationsSeq, value.timestamp))
      val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = value, oldReference =
        DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, oldObservationsSeq, value.timestamp))

      // add the value to the state
      pastObservations.update(oldObservationsSeq)
      pastObservations.add(value)

      // return the relative change observation
      out.collect(RelativeChangeObservation(value.measurementId, value.publishTimestamp, value, shortTermChange, longTermChange))
    }
  }


  override def open(parameters: Configuration): Unit = {
    pastObservations = getRuntimeContext.getListState(
      new ListStateDescriptor[AggregatableObservation]("previous-aggregated-obs", createTypeInformation[AggregatableObservation])
    )

    maxTimestamp = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("max-timestamp", createTypeInformation[Long], 0L)
    )
  }

  private def getCleanListOfOldObservations: Seq[AggregatableObservation] ={
    pastObservations.get().asScala.toSeq
      .filter(_.publishTimestamp > maxTimestamp.value() - settings.specific.maxOutOfOrderness - settings.general.longWindowLengthMillis)
  }
}