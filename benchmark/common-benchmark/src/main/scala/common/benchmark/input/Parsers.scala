package common.benchmark.input

import java.text.{DateFormat, SimpleDateFormat}

import common.benchmark.{FlowObservation, SpeedObservation}
import io.circe._
import io.circe.parser._
import cats.syntax.either._
import org.slf4j.LoggerFactory

/**
 * Common parsing, extracting, transforming utilities
 */
object Parsers extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)
  val inputConversionFunc = { input: String => input.toDouble }

  /**
   * Parses a speed observation line
   *
   * @param measurementId key of observation
   * @param value         observation
   * @param publishTime   time of publication on Kafka (should be in Kafka consumer record)
   * @return [[(String, SpeedObservation)]] key and observation
   */
  def parseLineSpeedObservation(measurementId: String, value: String, publishTime: Long, jobProfile: String): SpeedObservation = {
    val hCursor: HCursor = parse(value).getOrElse(Json.Null).hcursor

    val speedObservation = for {
      internalId <- hCursor.get[String]("internalId")
      latitude <- hCursor.get[Double]("lat")
      longitude <- hCursor.get[Double]("long")
      speed <- hCursor.get[Double]("speed")
      accuracy <- hCursor.get[Double]("accuracy")
      num_lanes <- hCursor.get[Double]("num_lanes")
    } yield SpeedObservation(
      measurementId,
      internalId,
      publishTime,
      latitude,
      longitude,
      speed.toDouble,
      accuracy.toInt,
      num_lanes.toInt,
      jobProfile
    )
    speedObservation.right.get
  }

  /**
   * Checks if speed and flow of a single measurement point has the same number of lanes
   *
   * @param speedLanes
   * @param flowLanes
   * @return Int describing the number of lanes (-1 in case flow and speed return non-equal lane numbers)
   */

  def checkConsistentLanes(flowLanes: Int, speedLanes: Int): Int = {
    if (speedLanes == flowLanes) speedLanes else -1
  }

  /**
   * Parses a flow observation line
   *
   * @param measurementId key of observation
   * @param value         observation
   * @param publishTime   time of publication on Kafka (should be in Kafka consumer record)
   * @return [[(String, FlowObservation)]] key and observation
   */
  def parseLineFlowObservation(measurementId: String, value: String, publishTime: Long, jobProfile: String): FlowObservation = {
    val hCursor: HCursor = parse(value).getOrElse(Json.Null).hcursor

    val flowObservation = for {
      internalId <- hCursor.get[String]("internalId")
      latitude <- hCursor.get[Double]("lat")
      longitude <- hCursor.get[Double]("long")
      flow <- hCursor.get[Double]("flow")
      period <- hCursor.get[Double]("period")
      accuracy <- hCursor.get[Double]("accuracy")
      num_lanes <- hCursor.get[Double]("num_lanes")
    } yield FlowObservation(
      measurementId,
      internalId,
      publishTime,
      latitude,
      longitude,
      flow.toInt,
      period.toInt,
      accuracy.toInt,
      num_lanes.toInt,
      jobProfile
    )
    flowObservation.right.get
  }

  /**
   * Rounds a timestamp down to seconds
   *
   * @param time timestamp in millis
   * @return Timestamp in millis on second-level
   */
  def roundMillisToSeconds(time: Long): Long = {
    Math.round(time / 1000.0) * 1000
  }

 }
