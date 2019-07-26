package kafka.benchmark.stages

import java.sql.Timestamp
import java.time.Duration

import common.benchmark.stages.AnalyticsStagesTemplate
import common.benchmark.{AggregatableObservation, DataScienceMaths, RelativeChangeObservation}
import kafka.benchmark.BenchmarkSettingsForKafkaStreams
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig


/**
  * Contains all methods required in the aggregation and windowing phase.
  *
  * @param settings Kafka-streams configuration properties
  */
class AnalyticsStages(settings: BenchmarkSettingsForKafkaStreams)
  extends Serializable with AnalyticsStagesTemplate {

  /**
    * Aggregates over the lanes and times computation time
    *
    * @param parsedAndJoinedStream a [[common.benchmark.FlowObservation]] and [[common.benchmark.SpeedObservation]] belonging to a measurement point
    * @return [[KStream]] of [[AggregatableObservation]]
    */
  def aggregationStage(parsedAndJoinedStream: KStream[String, AggregatableObservation], test: Boolean = false): KStream[Windowed[String], AggregatableObservation] = {
    val aggregatedStream: KStream[Windowed[String], AggregatableObservation] = parsedAndJoinedStream
      .groupBy { case (_: String, value: AggregatableObservation) =>
        value.measurementId + "/" + value.timestamp
      }(Grouped.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
      .windowedBy(TimeWindows.of(settings.general.publishIntervalMillis)
        .grace(Duration.ofMillis(settings.specific.gracePeriodMillis)))
      .reduce {
        case (aggregatedObservation1: AggregatableObservation, aggregatedObservation2: AggregatableObservation) =>
          aggregatedObservation1.combineObservations(aggregatedObservation2)
      }(Materialized.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
      .toStream
      .filter { case (key: Windowed[String], aggregatableObservation: AggregatableObservation) =>
      aggregatableObservation.numLanes == aggregatableObservation.lanes.size
    }

    aggregatedStream
  }

  /**
    * Computes the relative change of the current observation compared to previous ones.
    *
    * @param aggregatedStream [[KStream]] of [[AggregatableObservation]]
    * @return [[KStream]] of [[RelativeChangeObservation]]
    */
  def relativeChangeStage(aggregatedStream: KStream[Windowed[String], AggregatableObservation]): KStream[String, RelativeChangeObservation] = {
    val appendWrittenAsKafkaAggFunction = (key: String, v: AggregatableObservation, accumulator: List[AggregatableObservation]) => v +: accumulator

    val idWithListOfObs: KStream[Windowed[String], List[AggregatableObservation]] = aggregatedStream
      .groupBy { case obs: (Windowed[String], AggregatableObservation) => obs._2.measurementId
      }(Grouped.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
      .windowedBy(TimeWindows.of(settings.general.longWindowLengthMillis)
        .advanceBy(settings.general.windowSlideIntervalMillis)
        .grace(Duration.ofMillis(settings.specific.gracePeriodMillis)))
      .aggregate[List[AggregatableObservation]](
      initializer = List[AggregatableObservation]())(
      aggregator = appendWrittenAsKafkaAggFunction)(
      materialized = Materialized.as("relative-change-store")
        .withKeySerde(CustomObjectSerdes.StringSerde)
        .withValueSerde(CustomObjectSerdes.AggregatedObservationListSerde)
    )
      .toStream

    val relativeChangeStream: KStream[String, RelativeChangeObservation] = idWithListOfObs.flatMap[String, RelativeChangeObservation] { (key: Windowed[String], observations: List[AggregatableObservation]) => {
      val obs = observations.maxBy(_.timestamp)
      if (obs.timestamp >= key.window().endTime().toEpochMilli - settings.general.windowSlideIntervalMillis) {
        val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
          DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, observations, obs.timestamp))
        val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
          DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, observations, obs.timestamp))

        Some(obs.measurementId, RelativeChangeObservation(obs.measurementId, obs.publishTimestamp, obs, shortTermChange, longTermChange))
      } else None
    }
    }

    relativeChangeStream
  }
}
