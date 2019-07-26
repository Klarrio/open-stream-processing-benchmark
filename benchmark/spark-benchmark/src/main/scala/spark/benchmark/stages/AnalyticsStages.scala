package spark.benchmark.stages

import common.benchmark.{DataScienceMaths, _}
import common.benchmark.stages.AnalyticsStagesTemplate
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream
import spark.benchmark.BenchmarkSettingsForSpark

/**
  * Contains all methods required in the aggregation and windowing phase.
  *
  * @param sparkSession Spark session
  * @param settings     Spark configuration properties
  */
class AnalyticsStages(settings: BenchmarkSettingsForSpark) extends Serializable with AnalyticsStagesTemplate {

  /**
    * Aggregates over the lanes and times computation time
    *
    * - Takes a window of the long look-back period.
    * - Aggregates lanes of one measurement id at one point in time.
    *
    * @param parsedAndJoinedStream a [[FlowObservation]] and [[SpeedObservation]] belonging to a measurement point
    * @return [[DStream]] of [[AggregatableObservation]]
    */
  def aggregationStage(parsedAndJoinedStream: DStream[(String, (FlowObservation, SpeedObservation))]): DStream[AggregatableObservation] = {
    val observationstoAggregate = parsedAndJoinedStream.map { case (key: String, (flowObservation: FlowObservation, speedObservation: SpeedObservation)) =>
      ((flowObservation.measurementId, flowObservation.timestamp), new AggregatableObservation(flowObservation, speedObservation))
    }
    observationstoAggregate.groupByKeyAndWindow(Milliseconds(settings.specific.batchInterval))
      .transform { rddToAggregate =>
        aggregateOverLanes(rddToAggregate)
      }
  }

  /**
    * Aggregates lanes belonging to same measurement ID and timestamp.
    *
    * - Takes the average of the speed over all the lanes.
    * - Sums up the flow over all the lanes.
    * - Appends the maps and lists of timestamps and lanes.
    *
    * @param parsedAndJoinedStream [[FlowObservation]] and [[SpeedObservation]] for one lane for a measurement ID
    * @return [[RDD]] of [[AggregatableObservation]]
    */
  def aggregateOverLanes(parsedAndJoinedStream: RDD[((String, Long), Iterable[AggregatableObservation])]): RDD[AggregatableObservation] = {
    parsedAndJoinedStream.map {
      case (_, observations) =>
        observations.toList.reduce {
          (aggregatedObservation1: AggregatableObservation, aggregatedObservation2: AggregatableObservation) =>
            aggregatedObservation1.combineObservations(aggregatedObservation2)
        }
    }
  }

  /**
    * Computes the relative change of the current observation compared to previous ones.
    *
    * - Creates a reporter to time how long the windowing took.
    * - Computes the relative change over time in the window.
    *
    * @param aggregatedStream [[DStream]] of [[AggregatableObservation]]
    * @return [[DStream]] of [[RelativeChangeObservation]]
    */
  def relativeChangesStage(aggregatedStream: DStream[AggregatableObservation]): DStream[RelativeChangeObservation] = {
    aggregatedStream.map(obs => (obs.measurementId, obs))
      .groupByKeyAndWindow(Milliseconds(settings.general.longWindowLengthMillis), Milliseconds(settings.general.windowSlideIntervalMillis))
      .transform { rddToWindow =>
        computeRelativeChange(rddToWindow)
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
          case obs: AggregatableObservation => {
            val shortTermChange: Option[RelativeChangePercentages] = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
              DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, observationsList, obs.timestamp))
            val longTermChange: Option[RelativeChangePercentages] = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
              DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, observationsList, obs.timestamp))
            if (shortTermChange.isDefined && longTermChange.isDefined) {
              Some(RelativeChangeObservation(obs.measurementId, obs.publishTimestamp, obs, shortTermChange, longTermChange))
            } else None
          }
        }
    }
  }
}

