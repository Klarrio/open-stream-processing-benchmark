package common.benchmark

import java.sql.Timestamp

import common.benchmark.input.Parsers

object DataScienceMaths {
  def calculateRelativeChangeBetweenObservations(newest: AggregatableObservation, oldReference: Option[AggregatableObservation]): Option[RelativeChangePercentages] = {
    oldReference.map(oldValue => RelativeChangePercentages(
      flowPct = (newest.accumulatedFlow - oldValue.accumulatedFlow) / oldValue.accumulatedFlow.toDouble,
      speedPct = (newest.averageSpeed - oldValue.averageSpeed) / oldValue.averageSpeed.toDouble
    ))
  }

  def lookbackInTime(nbSeconds: Int, history: Seq[AggregatableObservation], referenceTimestampMillis: Long): Option[AggregatableObservation] = {
    history.find(x => x.roundedTimestamp == Parsers.roundMillisToSeconds(referenceTimestampMillis) - (nbSeconds * 1000))
  }

  def calculateRelativeChangeBetweenObservationsForStructuredStreaming(newest: AggregatableObservationWithTimestamp, oldReference: Option[AggregatableObservationWithTimestamp]): Option[RelativeChangePercentages] = {
    oldReference.map(oldValue => RelativeChangePercentages(
      flowPct = (newest.accumulatedFlow - oldValue.accumulatedFlow) / oldValue.accumulatedFlow.toDouble,
      speedPct = (newest.averageSpeed - oldValue.averageSpeed) / oldValue.averageSpeed.toDouble
    ))
  }

  def lookbackInTimeForStructuredStreaming(nbSeconds: Int, history: Seq[AggregatableObservationWithTimestamp], referenceTimestamp: Timestamp): Option[AggregatableObservationWithTimestamp] = {
    history .find(x => x.roundedTimestamp == Parsers.roundMillisToSeconds(referenceTimestamp.getTime) - (nbSeconds * 1000))
  }
}
