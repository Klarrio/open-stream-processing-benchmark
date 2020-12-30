package spark.benchmark.stages

import common.benchmark.{DataScienceMaths, _}
import common.benchmark.stages.StatefulStagesTemplate
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Milliseconds}
import org.apache.spark.streaming.dstream.DStream
import spark.benchmark.BenchmarkSettingsForSpark

/**
 * Contains all methods required in the aggregation and windowing phase.
 *
 * @param settings Spark configuration properties
 */
class StatefulStages(settings: BenchmarkSettingsForSpark) extends Serializable with StatefulStagesTemplate {


  /**
   * Joins the flow and speed streams
   *
   * If this is the last stage to execute, it publishes the observation on Kafka
   *
   * @param parsedFlowStream  [[DStream]] of [[FlowObservation]]
   * @param parsedSpeedStream [[DStream]] of [[SpeedObservation]]
   * @return [[DStream]] of [[FlowObservation]] and [[SpeedObservation]]
   */
  def joinStage(parsedFlowStream: DStream[(String, FlowObservation)], parsedSpeedStream: DStream[(String, SpeedObservation)]): DStream[(String, (FlowObservation, SpeedObservation))] = {
    parsedFlowStream
      .join(parsedSpeedStream)
  }


  /**
   * Incrementally aggregates over the lanes and times computation time
   *
   * - Takes a window of the long look-back period.
   * - Aggregates lanes of one measurement id at one point in time.
   *
   * @param parsedAndJoinedStream a [[FlowObservation]] and [[SpeedObservation]] belonging to a measurement point
   * @return [[DStream]] of [[AggregatableObservation]]
   */
  def aggregationAfterJoinStage(parsedAndJoinedStream: DStream[(String, (FlowObservation, SpeedObservation))]): DStream[AggregatableObservation] = {
    val observationstoAggregate = parsedAndJoinedStream.map { case (key: String, (flowObservation: FlowObservation, speedObservation: SpeedObservation)) =>
      // works with processing time so we need to put the event time in the aggregation key
      ((flowObservation.measurementId, flowObservation.roundedTimestamp), new AggregatableObservation(flowObservation, speedObservation))
    }
    observationstoAggregate
      .reduceByKeyAndWindow (
        (aggregatedObservation1: AggregatableObservation, aggregatedObservation2: AggregatableObservation) =>
          aggregatedObservation1.combineObservations(aggregatedObservation2),
        Milliseconds(settings.specific.batchInterval)
      ).map(_._2)
  }

  /**
   * Non-incrementally aggregates/computes the relative change of the current observation compared to previous ones.
   *
   * - Creates a reporter to time how long the windowing took.
   * - Computes the relative change over time in the window.
   *
   * @param aggregatedStream [[DStream]] of [[AggregatableObservation]]
   * @return [[DStream]] of [[RelativeChangeObservation]]
   */
  def slidingWindowAfterAggregationStage(aggregatedStream: DStream[AggregatableObservation]): DStream[RelativeChangeObservation] = {
    aggregatedStream.map(obs => (obs.measurementId, obs))
      .groupByKeyAndWindow(Milliseconds(settings.general.longWindowLengthMillis), Milliseconds(settings.general.publishIntervalMillis))
      .transform { rddToWindow =>
        computeRelativeChange(rddToWindow)
      }
  }


  /**
   * Incrementally aggregates over the lanes and times computation time
   *
   * - Takes a window of the long look-back period.
   * - Aggregates lanes of one measurement id at one point in time.
   *
   * @param parsedFlowStream a [[FlowObservation]] belonging to a measurement point
   * @return [[DStream]] of [[AggregatableFlowObservation]]
   */
  def reduceWindowAfterParsingStage(parsedFlowStream: DStream[(String, FlowObservation)]): DStream[(String, AggregatableFlowObservation)] = {
    val aggregatableFlowStream = parsedFlowStream.map { case (key: String, flowObservation: FlowObservation) =>
      (flowObservation.measurementId, new AggregatableFlowObservation(flowObservation))
    }
    val aggregatedFlowStream = aggregatableFlowStream
      .reduceByKeyAndWindow(
        (aggregatedFlowObservation1: AggregatableFlowObservation, aggregatedFlowObservation2: AggregatableFlowObservation) =>
          aggregatedFlowObservation1.combineObservations(aggregatedFlowObservation2),
        Milliseconds(settings.general.windowDurationMsOfWindowAfterParse),
        Milliseconds(settings.general.slideDurationMsOfWindowAfterParse)
      )
    aggregatedFlowStream
  }


  /**
   * Non-incrementally aggregates over the lanes and times computation time
   *
   * - Takes a window of the long look-back period.
   * - Aggregates lanes of one measurement id at one point in time.
   *
   * @param parsedFlowStream a [[FlowObservation]] belonging to a measurement point
   * @return [[DStream]] of [[AggregatableFlowObservation]]
   */
  def nonIncrementalWindowAfterParsingStage(parsedFlowStream: DStream[(String, FlowObservation)]): DStream[(String, AggregatableFlowObservation)] = {
    val aggregatableFlowStream = parsedFlowStream.map { case (key: String, flowObservation: FlowObservation) =>
      (flowObservation.measurementId, new AggregatableFlowObservation(flowObservation))
    }
    val aggregatedFlowStream = aggregatableFlowStream
      .groupByKeyAndWindow(Milliseconds(settings.general.windowDurationMsOfWindowAfterParse),
        Milliseconds(settings.general.slideDurationMsOfWindowAfterParse))
      .transform(computeNonIncrementalAggregation _)
    aggregatedFlowStream
  }

  /**
   * Non-incrementally aggregates lanes belonging to same measurement ID.
   *
   * - Sums up the flow over all the lanes and all timestamps of the window.
   *
   * @param parsedFlowStream [[FlowObservation]] parsed RDD of one measurement ID
   * @return [[RDD]] of [[AggregatableFlowObservation]]
   */
  def computeNonIncrementalAggregation(parsedFlowStream: RDD[(String, Iterable[AggregatableFlowObservation])]): RDD[(String, AggregatableFlowObservation)] = {
    parsedFlowStream.map {
      case (key, observations) =>
        val aggregationResult = observations.toList.reduce {
          (aggregatedFlowObservation1: AggregatableFlowObservation, aggregatedFlowObservation2: AggregatableFlowObservation) =>
            aggregatedFlowObservation1.combineObservations(aggregatedFlowObservation2)
        }
        (key, aggregationResult)
    }
  }

  /**
   * Computes the relative change of a certain measurement ID over time.
   *
   * Short and long term relative change is computed for flow and speed
   * [obs(t) - obs(t-n)]/obs(t-n)
   *
   * @param aggregatedStream [[RDD]] with [[AggregatableObservation]] per measurement ID for the entire long look-back window
   * @return [[RDD]] of [[RelativeChangeObservation]]
   */
  def computeRelativeChange(aggregatedStream: RDD[(String, Iterable[AggregatableObservation])]): RDD[RelativeChangeObservation] = {
    aggregatedStream.flatMap {
      case (_, observationsOfId) =>
        val observationsList: Seq[AggregatableObservation] = observationsOfId.toVector

        // Since Spark does not do event-time processing we compute this over the entire list of observations
        // We only output those observations which have a short and long lookback observation in the data
        // This way for normal workloads where every batch only contains data for this timestamp, we will not output the older observations over and over again but only the newest observation which has these two lookbacks
        // For single burst this will still make sure that the observations are outputted. In single burst the entire burst will end up in one window and otherwise they wouldn't all be outputted.
        observationsList.flatMap {
          obs: AggregatableObservation => {
            val shortTermChange: Option[RelativeChangePercentages] = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
              DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, observationsList, obs.publishTimestamp))
            val longTermChange: Option[RelativeChangePercentages] = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
              DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, observationsList, obs.publishTimestamp))
            if (shortTermChange.isDefined && longTermChange.isDefined) {
              Some(RelativeChangeObservation(obs.measurementId, obs.publishTimestamp, obs, shortTermChange, longTermChange, obs.jobProfile))
            } else None
          }
        }
    }
  }
}