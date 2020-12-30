package common.benchmark

import java.sql.Timestamp

import common.benchmark.input.Parsers
import common.benchmark.input.Parsers.checkConsistentLanes

final case class FlowObservation(
  measurementId: String,
  internalId: String,
  publishTimestamp: Long,
  latitude: Double,
  longitude: Double,
  flow: Int,
  period: Int,
  accuracy: Int,
  numLanes: Int,
  jobProfile: String
) extends Serializable {

  val roundedTimestamp = Parsers.roundMillisToSeconds(publishTimestamp)
}

final case class SpeedObservation(
  measurementId: String,
  internalId: String,
  publishTimestamp: Long,
  latitude: Double,
  longitude: Double,
  speed: Double,
  accuracy: Int,
  numLanes: Int,
  jobProfile: String
) extends Serializable {

  val roundedTimestamp = Parsers.roundMillisToSeconds(publishTimestamp)
}

case class AggregatableObservation(
  measurementId: String,
  lanes: List[String],
  publishTimestamp: Long,
  latitude: Double,
  longitude: Double,
  accumulatedFlow: Int,
  period: Int,
  flowAccuracy: Int,
  averageSpeed: Double,
  speedAccuracy: Int,
  numLanes: Int,
  jobProfile: String
) extends Serializable {

  val roundedTimestamp = Parsers.roundMillisToSeconds(publishTimestamp)

  def this(flow: FlowObservation, speed: SpeedObservation) {
    this(
      flow.measurementId,
      List(flow.internalId),
      Math.max(flow.publishTimestamp, speed.publishTimestamp),
      flow.latitude,
      flow.longitude,
      flow.flow,
      flow.period,
      flow.accuracy,
      speed.speed,
      speed.accuracy,
      checkConsistentLanes(flow.numLanes, speed.numLanes),
      flow.jobProfile
    )
  }

  /**
   * use this method to combine two aggregatedobservations into one.
   *   - sums up the flow and averages the speed
   *   - accumulates the lanes, publish and ingest timestamps
   */
  def combineObservations(aggregatedObservation: AggregatableObservation): AggregatableObservation = {

    this.copy(
      lanes = this.lanes ++ aggregatedObservation.lanes,
      publishTimestamp = Math.max(this.publishTimestamp, aggregatedObservation.publishTimestamp),
      //if period is not the same for both observations, make -1
      period = if (this.period == aggregatedObservation.period) this.period else -1,
      accumulatedFlow = this.accumulatedFlow + aggregatedObservation.accumulatedFlow,
      flowAccuracy = if (this.flowAccuracy == aggregatedObservation.flowAccuracy) this.flowAccuracy else -1,
      //computes the average speed over all the lanes of both observations
      averageSpeed = (this.averageSpeed * this.lanes.length + aggregatedObservation.averageSpeed *
        aggregatedObservation.lanes.length) / (this.lanes.length + aggregatedObservation.lanes.length),
      speedAccuracy = if (this.speedAccuracy == aggregatedObservation.speedAccuracy) this.speedAccuracy else -1
    )
  }
}

case class AggregatableFlowObservation(
  measurementId: String,
  laneCount: Int,
  publishTimestamp: Long,
  latitude: Double,
  longitude: Double,
  accumulatedFlow: Int,
  period: Int,
  flowAccuracy: Int,
  numLanes: Int,
  jobProfile: String
) extends Serializable {

  val roundedTimestamp = Parsers.roundMillisToSeconds(publishTimestamp)

  def this(flow: FlowObservation) {
    this(
      flow.measurementId,
      1,
      flow.publishTimestamp,
      flow.latitude,
      flow.longitude,
      flow.flow,
      flow.period,
      flow.accuracy,
      flow.numLanes,
      flow.jobProfile
    )
  }

  /**
   * use this method to combine two aggregatedobservations into one.
   *   - sums up the flow and averages the speed
   *   - accumulates the lanes, publish and ingest timestamps
   */
  def combineObservations(aggregatedObservation: AggregatableFlowObservation): AggregatableFlowObservation = {
    this.copy(
      laneCount = this.laneCount + aggregatedObservation.laneCount,
      publishTimestamp = Math.max(this.publishTimestamp, aggregatedObservation.publishTimestamp),
      //if period is not the same for both observations, make -1
      period = if (this.period == aggregatedObservation.period) this.period else -1,
      accumulatedFlow = this.accumulatedFlow + aggregatedObservation.accumulatedFlow,
      flowAccuracy = if (this.flowAccuracy == aggregatedObservation.flowAccuracy) this.flowAccuracy else -1,
    )
  }
}

case class AggregatableFlowObservationWithTimestamp(
  measurementId: String,
  laneCount: Int,
  timestamp: Timestamp,
  publishTimestamp: Timestamp,
  latitude: Double,
  longitude: Double,
  accumulatedFlow: Int,
  period: Int,
  flowAccuracy: Int,
  numLanes: Int
) extends Serializable  {  /**
 * use this method to combine two aggregatedobservations into one.
 *   - sums up the flow and averages the speed
 *   - accumulates the lanes, publish and ingest timestamps
 */
  def combineObservations(aggregatedObservation: AggregatableFlowObservationWithTimestamp): AggregatableFlowObservationWithTimestamp = {
    this.copy(
      laneCount = this.laneCount + aggregatedObservation.laneCount,
      timestamp = if (this.timestamp.after(aggregatedObservation.timestamp)) this.timestamp else aggregatedObservation.timestamp,
      publishTimestamp = if (this.publishTimestamp.after(aggregatedObservation.publishTimestamp)) this.publishTimestamp else aggregatedObservation.publishTimestamp,
      //if period is not the same for both observations, make -1
      period = if (this.period == aggregatedObservation.period) this.period else -1,
      accumulatedFlow = this.accumulatedFlow + aggregatedObservation.accumulatedFlow,
      flowAccuracy = if (this.flowAccuracy == aggregatedObservation.flowAccuracy) this.flowAccuracy else -1,
    )
  }
}

case class AggregatableObservationWithTimestamp(
  measurementId: String,
  lanes: List[String],
  timestamp: Timestamp,
  roundedTimestamp: Timestamp,
  publishTimestamp: Timestamp,
  latitude: Double,
  longitude: Double,
  accumulatedFlow: Int,
  period: Int,
  flowAccuracy: Int,
  averageSpeed: Double,
  speedAccuracy: Int,
  numLanes: Int
) extends Serializable {
  /**
   * use this method to combine two aggregatedobservations into one.
   *   - sums up the flow and averages the speed
   *   - accumulates the lanes, publish and ingest timestamps
   */
  def combineObservations(aggregatedObservation: AggregatableObservationWithTimestamp): AggregatableObservationWithTimestamp = {
    this.copy(
      lanes = this.lanes ++ aggregatedObservation.lanes,
      timestamp = if (this.timestamp.after(aggregatedObservation.timestamp)) this.timestamp else aggregatedObservation.timestamp,
      roundedTimestamp = if (this.roundedTimestamp.after(aggregatedObservation.roundedTimestamp)) this.roundedTimestamp else aggregatedObservation.roundedTimestamp,
      //if period is not the same for both observations, make -1
      period = if (this.period == aggregatedObservation.period) this.period else -1,
      accumulatedFlow = this.accumulatedFlow + aggregatedObservation.accumulatedFlow,
      flowAccuracy = if (this.flowAccuracy == aggregatedObservation.flowAccuracy) this.flowAccuracy else -1,
      //computes the average speed over all the lanes of both observations
      averageSpeed = (this.averageSpeed * this.lanes.length + aggregatedObservation.averageSpeed *
        aggregatedObservation.lanes.length) / (this.lanes.length + aggregatedObservation.lanes.length),
      speedAccuracy = if (this.speedAccuracy == aggregatedObservation.speedAccuracy) this.speedAccuracy else -1
    )
  }
}

case class RelativeChangePercentages(flowPct: Double, speedPct: Double)

case class RelativeChangeObservation(
  measurementId: String,
  publishTimestamp: Long,
  aggregatedObservation: AggregatableObservation,
  shortDiff: Option[RelativeChangePercentages],
  longDiff: Option[RelativeChangePercentages],
  jobProfile: String) extends Serializable

case class RelativeChangeObservationWithTimestamp(
  measurementId: String,
  publishTimestamp: Timestamp,
  aggregatedObservation: AggregatableObservationWithTimestamp,
  shortDiff: Option[RelativeChangePercentages],
  longDiff: Option[RelativeChangePercentages]) extends Serializable