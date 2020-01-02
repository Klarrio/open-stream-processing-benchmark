package common.benchmark

import java.sql.Timestamp

import play.api.libs.json.{Json, Reads, Writes, __}
import play.api.libs.functional.syntax._
import common.benchmark.input.Parsers.checkConsistentLanes
import play.api.libs.functional.syntax.unlift

trait Observation {
  def toJsonString(): String = {""}
}

final case class FlowObservation(
  measurementId: String,
  internalId: String,
  timestamp: Long,
  latitude: Double,
  longitude: Double,
  flow: Int,
  period: Int,
  accuracy: Int,
  numLanes: Int,
  publishTimestamp: Long,
  ingestTimestamp: Long
) extends Observation with Serializable {

  def this() = this("String", "String", 1L, 1.0, 1.0, 1, 1, 1, 1, 1L, 1L)

  override def toJsonString(): String = {
    implicit val NewFlowObservationWriter = Json.writes[FlowObservation]
    Json.toJson(this).toString()
  }
}

final case class SpeedObservation(
  measurementId: String,
  internalId: String,
  timestamp: Long,
  latitude: Double,
  longitude: Double,
  speed: Double,
  accuracy: Int,
  numLanes: Int,
  publishTimestamp: Long,
  ingestTimestamp: Long
) extends Observation with Serializable {

  def this() = this("String", "String", 1L, 1.0, 1.0, 1, 1, 1, 1L, 1L)

  override def toJsonString(): String = {
    implicit val NewSpeedObservationWriter = Json.writes[SpeedObservation]
    Json.toJson(this).toString()
  }

}

case class AggregatableObservation(
  measurementId: String,
  lanes: List[String],
  timestamp: Long,
  latitude: Double,
  longitude: Double,
  accumulatedFlow: Int,
  period: Int,
  flowAccuracy: Int,
  averageSpeed: Double,
  speedAccuracy: Int,
  numLanes: Int,
  publishTimestamp: Long,
  ingestTimestamp: Long
) extends Serializable with Observation {

  //If you want to create an aggregate observation from a flow object and speed object
  def this() = this("String", List("String"), 1L, 1.0, 1.0, 1, 1, 1, 1.0, 1, 1, 1l, 1l)

  def this(flow: FlowObservation, speed: SpeedObservation) {
    this(
      flow.measurementId,
      List(flow.internalId),
      Math.max(flow.timestamp, speed.timestamp),
      flow.latitude,
      flow.longitude,
      flow.flow,
      flow.period,
      flow.accuracy,
      speed.speed,
      speed.accuracy,
      checkConsistentLanes(flow.numLanes, speed.numLanes),
      Math.max(flow.publishTimestamp, speed.publishTimestamp),
      Math.max(flow.ingestTimestamp, speed.ingestTimestamp)
    )

  }

  override def toJsonString(): String = {
    implicit val AggObservationWriter = Json.writes[AggregatableObservation]
    Json.toJson(this).toString()
  }

  /**
    * use this method to combine two aggregatedobservations into one.
    *   - sums up the flow and averages the speed
    *   - accumulates the lanes, publish and ingest timestamps
    */

  def combineObservations(aggregatedObservation: AggregatableObservation): AggregatableObservation = {
    val maxPublishTimestamp = Math.max(this.publishTimestamp, aggregatedObservation.publishTimestamp)
    val maxIngestTimestamp = Math.max(this.ingestTimestamp, aggregatedObservation.ingestTimestamp)

    this.copy(
      lanes = this.lanes ++ aggregatedObservation.lanes,
      //if period is not the same for both observations, make -1
      period = if (this.period == aggregatedObservation.period) this.period else -1,
      accumulatedFlow = this.accumulatedFlow + aggregatedObservation.accumulatedFlow,
      flowAccuracy = if (this.flowAccuracy == aggregatedObservation.flowAccuracy) this.flowAccuracy else -1,
      //computes the average speed over all the lanes of both observations
      averageSpeed = (this.averageSpeed * this.lanes.length + aggregatedObservation.averageSpeed *
        aggregatedObservation.lanes.length) / (this.lanes.length + aggregatedObservation.lanes.length),
      speedAccuracy = if (this.speedAccuracy == aggregatedObservation.speedAccuracy) this.speedAccuracy else -1,
      publishTimestamp = maxPublishTimestamp,
      ingestTimestamp = maxIngestTimestamp
    )
  }
}

case class AggregatableObservationWithTimestamp(
  measurementId: String,
  lanes: List[String],
  timestamp: Timestamp,
  latitude: Double,
  longitude: Double,
  accumulatedFlow: Int,
  period: Int,
  flowAccuracy: Int,
  averageSpeed: Double,
  speedAccuracy: Int,
  numLanes: Int,
  publishTimestamp: Long,
  ingestTimestamp: Long
) extends Serializable with Observation {

  def this(flow: FlowObservation, speed: SpeedObservation) {
    this(
      flow.measurementId,
      List(flow.internalId),
      if(flow.timestamp>speed.timestamp) new Timestamp(flow.timestamp) else new Timestamp(speed.timestamp),
      flow.latitude,
      flow.longitude,
      flow.flow,
      flow.period,
      flow.accuracy,
      speed.speed,
      speed.accuracy,
      checkConsistentLanes(flow.numLanes, speed.numLanes),
      Math.max(flow.publishTimestamp, speed.publishTimestamp),
      Math.max(flow.ingestTimestamp, speed.ingestTimestamp)
    )

  }


  /**
    * use this method to combine two aggregatedobservations into one.
    *   - sums up the flow and averages the speed
    *   - accumulates the lanes, publish and ingest timestamps
    */

  def combineObservations(aggregatedObservation: AggregatableObservationWithTimestamp): AggregatableObservationWithTimestamp = {
    val maxPublishTimestamp = Math.max(this.publishTimestamp, aggregatedObservation.publishTimestamp)
    val maxIngestTimestamp = Math.max(this.ingestTimestamp, aggregatedObservation.ingestTimestamp)

    this.copy(
      lanes = this.lanes ++ aggregatedObservation.lanes,
      //if period is not the same for both observations, make -1
      period = if (this.period == aggregatedObservation.period) this.period else -1,
      accumulatedFlow = this.accumulatedFlow + aggregatedObservation.accumulatedFlow,
      flowAccuracy = if (this.flowAccuracy == aggregatedObservation.flowAccuracy) this.flowAccuracy else -1,
      //computes the average speed over all the lanes of both observations
      averageSpeed = (this.averageSpeed * this.lanes.length + aggregatedObservation.averageSpeed *
        aggregatedObservation.lanes.length) / (this.lanes.length + aggregatedObservation.lanes.length),
      speedAccuracy = if (this.speedAccuracy == aggregatedObservation.speedAccuracy) this.speedAccuracy else -1,
      publishTimestamp = maxPublishTimestamp,
      ingestTimestamp = maxIngestTimestamp
    )
  }
}


case class RelativeChangePercentages(flowPct: Double, speedPct: Double)

case class RelativeChangeObservation(
  measurementId: String,
  publishTimestamp: Long,
  aggregatedObservation: AggregatableObservation,
  shortDiff: Option[RelativeChangePercentages],
  longDiff: Option[RelativeChangePercentages]) extends Serializable with Observation {

  override def toJsonString(): String = {
    Json.toJson(this)(RelativeChangeObservationtoJSONHelpers.relativeChangeObservationWriter).toString()
  }
}

object RelativeChangeObservationtoJSONHelpers {
  implicit val aggregatedObservationReader: Reads[AggregatableObservation] = Json.reads[AggregatableObservation]
  implicit val relativeChangePercentagesReader: Reads[RelativeChangePercentages] = Json.reads[RelativeChangePercentages]
  implicit val relativeChangeObservationReader: Reads[RelativeChangeObservation] = Json.reads[RelativeChangeObservation]

  implicit val aggregatedObservationWriter = Json.writes[AggregatableObservation]
  implicit val relativeChangePercentagesWriter: Writes[RelativeChangePercentages] = Json.writes[RelativeChangePercentages]
  implicit val relativeChangeObservationWriter: Writes[RelativeChangeObservation] = Json.writes[RelativeChangeObservation]
}

case class RelativeChangeObservationWithTimestamp(
  measurementId: String,
  publishTimestamp: Long,
  aggregatedObservation: AggregatableObservationWithTimestamp,
  shortDiff: Option[RelativeChangePercentages],
  longDiff: Option[RelativeChangePercentages]) extends Serializable with Observation