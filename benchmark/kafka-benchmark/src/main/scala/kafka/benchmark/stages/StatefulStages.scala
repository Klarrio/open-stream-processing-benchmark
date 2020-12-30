package kafka.benchmark.stages

import java.time.Duration

import common.benchmark.stages.StatefulStagesTemplate
import common.benchmark.{AggregatableFlowObservation, AggregatableObservation, DataScienceMaths, FlowObservation, RelativeChangeObservation, SpeedObservation}
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
class StatefulStages(settings: BenchmarkSettingsForKafkaStreams)
  extends Serializable with StatefulStagesTemplate {

  /**
   * Joins the flow and speed streams
   *
   * @param parsedFlowStream  [[KStream]] of [[FlowObservation]]
   * @param parsedSpeedStream [[KStream]] of [[SpeedObservation]]
   * @return [[KStream]] of [[FlowObservation]] and [[SpeedObservation]]
   */
  def joinStage(parsedFlowStream: KStream[String, FlowObservation], parsedSpeedStream: KStream[String, SpeedObservation]): KStream[String, AggregatableObservation] = {
    val joinedSpeedAndFlowStreams = parsedFlowStream.selectKey((_, flowObservation) => flowObservation.measurementId + "/" + flowObservation.internalId + "/" + flowObservation.roundedTimestamp)
      .join(parsedSpeedStream.selectKey((_, speedObservation) => speedObservation.measurementId + "/" + speedObservation.internalId + "/" + speedObservation.roundedTimestamp))({
        (v1: FlowObservation, v2: SpeedObservation) => new AggregatableObservation(v1, v2)
      }, JoinWindows.of(Duration.ofMillis(settings.general.publishIntervalMillis))
        .grace(Duration.ofMillis(settings.specific.gracePeriodMillis))
        .until(settings.general.publishIntervalMillis * 2 + settings.specific.gracePeriodMillis))(
        StreamJoined.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.FlowObservationSerde, CustomObjectSerdes.SpeedObservationSerde))

    joinedSpeedAndFlowStreams
  }

  /**
   * Incrementally aggregates over the lanes and times computation time
   *
   * @param parsedAndJoinedStream a [[common.benchmark.FlowObservation]] and [[common.benchmark.SpeedObservation]] belonging to a measurement point
   * @return [[KStream]] of [[AggregatableObservation]]
   */
  def aggregationAfterJoinStage(parsedAndJoinedStream: KStream[String, AggregatableObservation]): KStream[Windowed[String], AggregatableObservation] = {
    parsedAndJoinedStream
      .groupBy { case (_: String, value: AggregatableObservation) =>
        value.measurementId + "/" + value.roundedTimestamp
      }(Grouped.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
      .windowedBy(TimeWindows.of(Duration.ofMillis(settings.general.publishIntervalMillis))
        .grace(Duration.ofMillis(settings.specific.gracePeriodMillis)))
      .reduce {
        case (aggregatedObservation1: AggregatableObservation, aggregatedObservation2: AggregatableObservation) =>
          aggregatedObservation1.combineObservations(aggregatedObservation2)
      }(Materialized.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde)
        .withRetention(Duration.ofMillis(settings.general.publishIntervalMillis + settings.specific.gracePeriodMillis))
        .withCachingEnabled())
      //        .suppress(Suppressed.untilTimeLimit(Duration.ofMillis(settings.general.publishIntervalMillis), Suppressed.BufferConfig.unbounded()))
      .toStream
      .filter { case (key: Windowed[String], aggregatableObservation: AggregatableObservation) =>
        aggregatableObservation.numLanes == aggregatableObservation.lanes.size
      }
  }


  /**
   * Incrementally aggregates over the lanes and times computation time
   *
   * @param parsedAndJoinedStream a [[common.benchmark.FlowObservation]] and [[common.benchmark.SpeedObservation]] belonging to a measurement point
   * @return [[KStream]] of [[AggregatableObservation]]
   */
  def lowLevelAggregationAfterJoinStage(parsedAndJoinedStream: KStream[String, AggregatableObservation]): KStream[String, AggregatableObservation] = {
    val aggregatedStream: KStream[String, AggregatableObservation] = parsedAndJoinedStream
      .selectKey { case (_: String, value: AggregatableObservation) =>
        value.measurementId + "/" + value.roundedTimestamp
      }.repartition(Repartitioned.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde).withName(settings.specific.aggregationThroughTopic))
      .transform(new LaneAggregator(settings), settings.specific.aggregatorStateStore)

    aggregatedStream
  }

  /**
   * Non-incrementally aggregates/computes the relative change of the current observation compared to previous ones.
   *
   * @param aggregatedStream [[KStream]] of [[AggregatableObservation]]
   * @return [[KStream]] of [[RelativeChangeObservation]]
   */
  def slidingWindowAfterAggregationStage(aggregatedStream: KStream[Windowed[String], AggregatableObservation]): KStream[String, RelativeChangeObservation] = {
    val appendAggregatableObservations = (key: String, v: AggregatableObservation, accumulator: List[AggregatableObservation]) => v +: accumulator
    aggregatedStream
      .groupBy { case obs: (Windowed[String], AggregatableObservation) => obs._2.measurementId
      }(Grouped.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde))
      .windowedBy(TimeWindows.of(Duration.ofMillis(settings.general.longWindowLengthMillis))
        .advanceBy(Duration.ofMillis(settings.general.publishIntervalMillis))
        .grace(Duration.ofMillis(0)))
      .aggregate[List[AggregatableObservation]](
        initializer = List[AggregatableObservation]())(
        aggregator = appendAggregatableObservations)(
        materialized = Materialized.as("relative-change-store")
          .withKeySerde(CustomObjectSerdes.StringSerde)
          .withValueSerde(CustomObjectSerdes.AggregatedObservationListSerde)
          .withRetention(Duration.ofMillis(settings.general.longWindowLengthMillis + settings.specific.gracePeriodMillis))
      )
      .toStream
      .flatMap {
        case in: (Windowed[String], List[AggregatableObservation]) =>
          val key = in._1
          val observations = in._2
          val latestObs = observations.maxBy(_.publishTimestamp)

          // Only return output for the keys that were in the last slide part of the window
          // So where the publishtimestamp of the observation is between the beginning of this slide and the end of the window
          if (latestObs.publishTimestamp >= key.window().end() - settings.general.publishIntervalMillis) {
            val shortTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = latestObs, oldReference =
              DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, observations, latestObs.publishTimestamp))
            val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = latestObs, oldReference =
              DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, observations, latestObs.publishTimestamp))
            Some(latestObs.measurementId, RelativeChangeObservation(latestObs.measurementId, latestObs.publishTimestamp, latestObs, shortTermChange, longTermChange, latestObs.jobProfile))
          } else None
      }
  }

  /**
   * Non-incrementally aggregates/computes the relative change of the current observation compared to previous ones.
   *
   * @param aggregatedStream [[KStream]] of [[AggregatableObservation]]
   * @return [[KStream]] of [[RelativeChangeObservation]]
   */
  def lowLevelSlidingWindowAfterAggregationStage(aggregatedStream: KStream[String, AggregatableObservation]): KStream[String, RelativeChangeObservation] = {
    val relativeChangeStream: KStream[String, RelativeChangeObservation] = aggregatedStream.selectKey { case obs: (String, AggregatableObservation) => obs._2.measurementId }
      .repartition(Repartitioned.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableObservationSerde).withName("relative-change-data-topic"))
      .transform(new RelativeChangeComputer(settings), settings.specific.relativeChangeStateStore)
    relativeChangeStream
  }


  /**
   * Incrementally aggregates over the lanes and times computation time
   *
   * - Takes a window of the long look-back period.
   * - Aggregates lanes of one measurement id at one point in time.
   *
   * @param parsedFlowStream a [[FlowObservation]] and [[SpeedObservation]] belonging to a measurement point
   * @return [[KStream]] of [[AggregatableFlowObservation]]
   */
  def reduceWindowAfterParsingStage(parsedFlowStream: KStream[String, FlowObservation]): KStream[Windowed[String], AggregatableFlowObservation] = {
    val aggregatableFlowStream: KStream[String, AggregatableFlowObservation] = parsedFlowStream.mapValues(el => new AggregatableFlowObservation(el))

    val aggregatedFlowStream = aggregatableFlowStream.groupBy { case (_: String, value: AggregatableFlowObservation) =>
      value.measurementId
    }(Grouped.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableFlowObservationSerde))
      .windowedBy(TimeWindows.of(Duration.ofMillis(settings.general.windowDurationMsOfWindowAfterParse))
        .advanceBy(Duration.ofMillis(settings.general.slideDurationMsOfWindowAfterParse))
        .grace(Duration.ofMillis(settings.specific.gracePeriodMillis)))
      .reduce {
        case (aggregatedFlowObservation1: AggregatableFlowObservation, aggregatedFlowObservation2: AggregatableFlowObservation) =>
          val reducedValue = aggregatedFlowObservation1.combineObservations(aggregatedFlowObservation2)
          reducedValue
      }(
        Materialized.as(settings.specific.reduceWindowAfterParsingStateStore)
          .withKeySerde(CustomObjectSerdes.StringSerde)
          .withValueSerde(CustomObjectSerdes.AggregatableFlowObservationSerde)
          .withRetention(Duration.ofMillis(settings.general.windowDurationMsOfWindowAfterParse + settings.specific.gracePeriodMillis)))
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
      .toStream

    aggregatedFlowStream
  }

  /**
   * Non-incrementally aggregates over the lanes and times computation time
   *
   * - Takes a window of the long look-back period.
   * - Aggregates lanes of one measurement id at one point in time.
   *
   * @param parsedFlowStream a [[FlowObservation]] and [[SpeedObservation]] belonging to a measurement point
   * @return [[KStream]] of [[AggregatableFlowObservation]]
   */
  def nonIncrementalWindowAfterParsingStage(parsedFlowStream: KStream[String, FlowObservation]): KStream[String, AggregatableFlowObservation] = {
    val aggregatableFlowStream: KStream[String, AggregatableFlowObservation] = parsedFlowStream.mapValues(el => new AggregatableFlowObservation(el))

    //    val appendFlowObservations = (key: String, v: AggregatableFlowObservation, accumulator: List[AggregatableFlowObservation]) => v +: accumulator

    val aggregatedFlowStream = aggregatableFlowStream.selectKey { case (_: String, value: AggregatableFlowObservation) =>
      value.measurementId}
      // repartitioning isn't necessary because measurementId is the key of the incoming stream
//      .repartition(Repartitioned.`with`(CustomObjectSerdes.StringSerde, CustomObjectSerdes.AggregatableFlowObservationSerde).withName(settings.specific.nonIncrementalWindowThroughTopic))
      .transform(new NonIncrementalAggregationComputer(settings), settings.specific.nonIncrementalWindowAfterParsingStateStore)

    aggregatedFlowStream
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
      this.context.schedule(Duration.ofMillis(10000), PunctuationType.STREAM_TIME, new Punctuator {

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

      kvStore = ctx.getStateStore(settings.specific.relativeChangeStateStore).asInstanceOf[KeyValueStore[String, List[AggregatableObservation]]]

      // periodically clean up state
      this.context.schedule(Duration.ofMillis(10000), PunctuationType.STREAM_TIME, new Punctuator {

        override def punctuate(timestamp: Long): Unit = {
          logger.debug("Inner punctuate call")

          val kvStoreIterator = kvStore.all()
          while (kvStoreIterator.hasNext) {
            val entry = kvStoreIterator.next()
            val filteredAggregatedObservationList = entry.value
              .filter(_.publishTimestamp > context.timestamp() - settings.general.longWindowLengthMillis - settings.specific.gracePeriodMillis)
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
            DataScienceMaths.lookbackInTime(settings.general.shortTermBatchesLookback, oldObservations, value.publishTimestamp))
          val longTermChange = DataScienceMaths.calculateRelativeChangeBetweenObservations(newest = value, oldReference =
            DataScienceMaths.lookbackInTime(settings.general.longTermBatchesLookback, oldObservations, value.publishTimestamp))
          context.forward(value.measurementId, RelativeChangeObservation(value.measurementId, value.publishTimestamp, value, shortTermChange, longTermChange, value.jobProfile))

          // add the value to the state
          val updatedValue = value :: oldObservations
          kvStore.put(key, updatedValue)

          // return the relative change observation
        } else {
          context.forward(value.measurementId, RelativeChangeObservation(value.measurementId, value.publishTimestamp, value, None, None, value.jobProfile))
          kvStore.put(key, List(value))
        }
      }
      null
    }

    override def close(): Unit = {
    }

  }
}


class NonIncrementalAggregationComputer(settings: BenchmarkSettingsForKafkaStreams) extends TransformerSupplier[String, AggregatableFlowObservation, KeyValue[String, AggregatableFlowObservation]] {
  override def get(): Transformer[String, AggregatableFlowObservation, KeyValue[String, AggregatableFlowObservation]] = new Transformer[String, AggregatableFlowObservation, KeyValue[String, AggregatableFlowObservation]] {
    private final val logger = LoggerFactory.getLogger(this.getClass)

    var context: ProcessorContext = _
    var kvStore: KeyValueStore[String, List[AggregatableFlowObservation]] = _

    override def init(ctx: ProcessorContext): Unit = {
      context = ctx

      kvStore = ctx.getStateStore(settings.specific.nonIncrementalWindowAfterParsingStateStore).asInstanceOf[KeyValueStore[String, List[AggregatableFlowObservation]]]

      // every slide duration, send out the results of the aggregations
      // and clean up state
      ctx.schedule(Duration.ofMillis(settings.general.slideDurationMsOfWindowAfterParse), PunctuationType.STREAM_TIME, new Punctuator {
        override def punctuate(timestamp: Long): Unit = sendOutOutputAndCleanState()
      })
    }


    override def transform(key: String, value: AggregatableFlowObservation): KeyValue[String, AggregatableFlowObservation] = {
      val oldObservations: List[AggregatableFlowObservation] = kvStore.get(key)

      if (oldObservations != null) {
        val updatedValue = value :: oldObservations
        kvStore.put(key, updatedValue)
      } else {
        kvStore.put(key, List(value))
      }

      null
    }

    /**
     * Reduce events and send out output for the PREVIOUS minute
     */
    def sendOutOutputAndCleanState(): Unit = {
      logger.debug("Sending output for minute: " + context.timestamp())

      val kvStoreIterator = kvStore.all()

      while (kvStoreIterator.hasNext) {
        val keyWithState = kvStoreIterator.next()

        // we need to filter out the events which have the current timestamp as timestamp because they actually already belong to the next window
        val previousMinuteEvents = keyWithState.value
          .filter(_.publishTimestamp < context.timestamp())

        if(previousMinuteEvents.nonEmpty){
          val aggregationResult = previousMinuteEvents.reduce {
            (aggregatableFlowObservation1, aggregatableFlowObservation2) =>
              aggregatableFlowObservation1.combineObservations(aggregatableFlowObservation2)
          }
          context.forward(keyWithState.key, aggregationResult)
        }

        // the state only needs to contain the values that need to be used for the next window computation
        // end time of the next window = context.timestamp() + settings.general.slideDurationMsOfWindowAfterParse
        val filteredAggregatedObservationList = keyWithState.value
          .filter(_.publishTimestamp > context.timestamp() + settings.general.slideDurationMsOfWindowAfterParse - settings.general.windowDurationMsOfWindowAfterParse)

        if (filteredAggregatedObservationList.isEmpty) {
          kvStore.delete(keyWithState.key)
        } else {
          kvStore.put(keyWithState.key, filteredAggregatedObservationList)
        }
      }
      kvStoreIterator.close()

      context.commit()
    }
    override def close(): Unit = {
    }
  }
}

