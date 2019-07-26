package common.benchmark

object DataScienceMaths {
  def calculateRelativeChangeBetweenObservations(newest: AggregatableObservation, oldReference: Option[AggregatableObservation]): Option[RelativeChangePercentages] = {
    oldReference.map(oldValue => RelativeChangePercentages(
      flowPct = (newest.accumulatedFlow - oldValue.accumulatedFlow) / oldValue.accumulatedFlow.toDouble,
      speedPct = (newest.averageSpeed - oldValue.averageSpeed) / oldValue.averageSpeed.toDouble
    )
    )
  }

  def lookbackInTime(nbSeconds: Int, history: Seq[AggregatableObservation], referenceTimestampMillis: Long): Option[AggregatableObservation] = {
    history .find(x => x.timestamp == referenceTimestampMillis - (nbSeconds * 1000))
  }
}
