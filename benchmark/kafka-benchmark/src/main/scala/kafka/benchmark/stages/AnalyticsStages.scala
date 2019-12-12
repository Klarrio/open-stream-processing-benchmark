package kafka.benchmark.stages

import java.time.Duration

import common.benchmark.stages.AnalyticsStagesTemplate
import common.benchmark.{AggregatableObservation, DataScienceMaths, RelativeChangeObservation}
import kafka.benchmark.BenchmarkSettingsForKafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory



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
  def aggregationStage(parsedAndJoinedStream: KStream[String, AggregatableObservation], test: Boolean = false): KStream[String, AggregatableObservation] = {

    val aggregatedStream: KStream[String, AggregatableObservation] =
      if (settings.specific.useCustomTumblingWindow) {
        parsedAndJoinedStream
          .selectKey { case (_: String, value: AggregatableObservation) =>
            value.measurementId + "/" + value.timestamp
          }.through("aggregation-data-topic")(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
          .transform(new LaneAggregator(settings), "lane-aggregator-state-store")
      }
      else {
        parsedAndJoinedStream
          .groupBy { case (_: String, value: AggregatableObservation) =>
            value.measurementId + "/" + value.timestamp
          }(Grouped.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
          .windowedBy(TimeWindows.of(settings.general.publishIntervalMillis)
            .advanceBy(settings.general.publishIntervalMillis)
            .grace(Duration.ofMillis(settings.specific.gracePeriodMillis))
            .until(settings.general.publishIntervalMillis + settings.specific.gracePeriodMillis))
          .reduce {
            case (aggregatedObservation1: AggregatableObservation, aggregatedObservation2: AggregatableObservation) =>
              aggregatedObservation1.combineObservations(aggregatedObservation2)
          }(Materialized.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
          .toStream
          .filter { case (key: Windowed[String], aggregatableObservation: AggregatableObservation) =>
            aggregatableObservation.numLanes == aggregatableObservation.lanes.size
          }.selectKey { case obs: (Windowed[String], AggregatableObservation) => obs._2.measurementId }
      }

    aggregatedStream
  }

  /**
    * Computes the relative change of the current observation compared to previous ones.
    *
    * @param aggregatedStream [[KStream]] of [[AggregatableObservation]]
    * @return [[KStream]] of [[RelativeChangeObservation]]
    */
  def relativeChangeStage(aggregatedStream: KStream[String, AggregatableObservation]): KStream[String, RelativeChangeObservation] = {
    val relativeChangeStream = if (settings.specific.useCustomSlidingWindow) {
      aggregatedStream.through("relative-change-data-topic")(Produced.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
        .transform(new RelativeChangeComputer(settings), "relative-change-state-store")
    } else {
      val appendWrittenAsKafkaAggFunction = (key: String, v: AggregatableObservation, accumulator: List[AggregatableObservation]) => v +: accumulator

      aggregatedStream
        .groupBy { case obs: (String, AggregatableObservation) => obs._2.measurementId
        }(Grouped.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
        .windowedBy(TimeWindows.of(settings.general.longWindowLengthMillis)
          .advanceBy(settings.general.windowSlideIntervalMillis)
          .grace(Duration.ofMillis(50))
          .until(settings.general.longWindowLengthMillis + settings.specific.gracePeriodMillis))
        .aggregate[List[AggregatableObservation]](
        initializer = List[AggregatableObservation]())(
        aggregator = appendWrittenAsKafkaAggFunction)(
        materialized = Materialized.as("relative-change-store")
          .withKeySerde(CustomObjectSerdes.StringSerde)
          .withValueSerde(CustomObjectSerdes.AggregatedObservationListSerde)
      )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream.mapValues {
        observations: List[AggregatableObservation] =>
          val obs = observations.maxBy(_.timestamp)

          val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
            DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, observations, obs.timestamp))
          val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = obs, oldReference =
            DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, observations, obs.timestamp))
          RelativeChangeObservation(obs.measurementId, obs.publishTimestamp, obs, shortTermChange, longTermChange)
      }
    }.selectKey { case obs: (Windowed[String], RelativeChangeObservation) => obs._2.measurementId }

    relativeChangeStream
  }
}


class LaneAggregator(settings: BenchmarkSettingsForKafkaStreams) extends TransformerSupplier[String, AggregatableObservation, KeyValue[String, AggregatableObservation]] {
  override def get(): Transformer[String, AggregatableObservation, KeyValue[String, AggregatableObservation]] = new Transformer[String, AggregatableObservation, KeyValue[String, AggregatableObservation]] {
    private final val logger = LoggerFactory.getLogger(this.getClass)

    var context: ProcessorContext = _
    var kvStore: KeyValueStore[String, AggregatableObservation] = _


    override def init(ctx: ProcessorContext): Unit = {
      context = ctx

      kvStore = ctx.getStateStore("lane-aggregator-state-store").asInstanceOf[KeyValueStore[String, AggregatableObservation]]

      // periodically clean up state
      this.context.schedule(10000, PunctuationType.STREAM_TIME, new Punctuator {

        override def punctuate(timestamp: Long): Unit = {
          logger.debug("Inner punctuate call")

          val kvStoreIterator = kvStore.all()
          while (kvStoreIterator.hasNext) {
            val entry = kvStoreIterator.next()
            if (entry.value.publishTimestamp < timestamp - settings.general.publishIntervalMillis - settings.specific.gracePeriodMillis) {
              kvStore.delete(entry.key)
            }
          }
          kvStoreIterator.close()

          context.commit()
        }
      })
    }

    override def transform(key: String, value: AggregatableObservation): KeyValue[String, AggregatableObservation] = {
      // In the case of only one lane the numLanes will be equal to the length, will be equal to 1
      // then just send out the observation
      if (value.numLanes == value.lanes.length) {
        context.forward(value.measurementId, value)
      } else {
        val oldValue = kvStore.get(key)

        if (oldValue != null) {
          val aggregatedValue = oldValue.combineObservations(value)

          if (aggregatedValue.numLanes == aggregatedValue.lanes.length) {
            context.forward(value.measurementId, aggregatedValue)
            kvStore.delete(key)
          } else {
            kvStore.put(key, aggregatedValue)
          }
        } else {
          kvStore.put(key, value)
        }
      }
      null
    }

    override def close(): Unit = {
    }
  }
}


class RelativeChangeComputer(settings: BenchmarkSettingsForKafkaStreams) extends TransformerSupplier[String, AggregatableObservation, KeyValue[String, RelativeChangeObservation]] {
  override def get(): Transformer[String, AggregatableObservation, KeyValue[String, RelativeChangeObservation]] = new Transformer[String, AggregatableObservation, KeyValue[String, RelativeChangeObservation]] {
    private final val logger = LoggerFactory.getLogger(this.getClass)

    var context: ProcessorContext = _
    var kvStore: KeyValueStore[String, List[AggregatableObservation]] = _


    override def init(ctx: ProcessorContext): Unit = {
      context = ctx

      kvStore = ctx.getStateStore("relative-change-state-store").asInstanceOf[KeyValueStore[String, List[AggregatableObservation]]]

      // periodically clean up state
      this.context.schedule(10000, PunctuationType.STREAM_TIME, new Punctuator {

        override def punctuate(timestamp: Long): Unit = {
          logger.debug("Inner punctuate call")

          val kvStoreIterator = kvStore.all()
          while (kvStoreIterator.hasNext) {
            val entry = kvStoreIterator.next()
            val filteredAggregatedObservationList = entry.value
              .filter(_.publishTimestamp > context.timestamp()- settings.general.longWindowLengthMillis - settings.specific.gracePeriodMillis )
            if (filteredAggregatedObservationList.isEmpty) {
              kvStore.delete(entry.key)
            } else {
              kvStore.put(entry.key, filteredAggregatedObservationList)
            }
          }
          kvStoreIterator.close()

          context.commit()
        }
      })
    }

    override def transform(key: String, value: AggregatableObservation): KeyValue[String, RelativeChangeObservation] = {
      // process the observation if it is newer than the low watermark
      if (value.publishTimestamp > context.timestamp() - settings.general.publishIntervalMillis - settings.specific.gracePeriodMillis) {

        val oldObservations: List[AggregatableObservation] = kvStore.get(key)

        if (oldObservations != null) {
          val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = value, oldReference =
            DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, oldObservations, value.timestamp))
          val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = value, oldReference =
            DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, oldObservations, value.timestamp))
          context.forward(value.measurementId, RelativeChangeObservation(value.measurementId, value.publishTimestamp, value, shortTermChange, longTermChange))

          // add the value to the state
          val updatedValue = value :: oldObservations
          kvStore.put(key, updatedValue)

          // return the relative change observation
        } else {
          context.forward(value.measurementId, RelativeChangeObservation(value.measurementId, value.publishTimestamp, value, None, None))
          kvStore.put(key, List(value))
        }
      }
      null
    }

    override def close(): Unit = {
    }

  }
}

