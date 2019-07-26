package common.benchmark.input

import java.text.{DateFormat, SimpleDateFormat}
import java.time.Instant

import common.benchmark.{FlowObservation, SpeedObservation}

import scala.util.parsing.json.JSON

/**
 * Common parsing, extracting, transforming utilities
 */
object Parsers extends Serializable {
  val inputConversionFunc = { input: String => input.toDouble }
  JSON.globalNumberParser = inputConversionFunc

  /**
   * Parses a speed observation line
   *
   * @param key         key of observation
   * @param value       observation
   * @param publishTime time of publication on Kafka (should be in Kafka consumer record)
   * @return [[(String, SpeedObservation)]] key and observation
   */
  def parseLineSpeedObservation(key: String, value: String, publishTime: Long): (String, SpeedObservation) = {
    val jsonMap = JSON.parseFull(value).get.asInstanceOf[Map[String, Any]]
    val latitude = jsonMap("lat").asInstanceOf[Double]
    val longitude = jsonMap("long").asInstanceOf[Double]
    val speed = jsonMap("speed").asInstanceOf[Double]
    val accuracy = jsonMap("accuracy").asInstanceOf[Double]
    val timestampString = jsonMap("timestamp").asInstanceOf[String]
    val num_lanes = jsonMap("num_lanes").asInstanceOf[Double]

    //Time rounded down to the minute
    val timestamp = getTimeInSeconds(timestampString)

    val splittedKey = key.splitAt(key.lastIndexOf("/"))
    val measurementId = splittedKey._1
    val internalId = splittedKey._2.replace("/", "")
    val keyObservation = measurementId + "/" + internalId + "/" + timestamp

    (
      keyObservation,
      SpeedObservation(
        measurementId,
        internalId,
        timestamp,
        latitude,
        longitude,
        speed.toDouble,
        accuracy.toInt,
        num_lanes.toInt,
        publishTime,
        Instant.now().toEpochMilli
      )
    )
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
   * @param key         key of observation
   * @param value       observation
   * @param publishTime time of publication on Kafka (should be in Kafka consumer record)
   * @return [[(String, FlowObservation)]] key and observation
   */
  def parseLineFlowObservation(key: String, value: String, publishTime: Long): (String, FlowObservation) = {
    val jsonMap = JSON.parseFull(value).get.asInstanceOf[Map[String, Any]]

    val latitude = jsonMap("lat").asInstanceOf[Double]
    val longitude = jsonMap("long").asInstanceOf[Double]
    val flow = jsonMap("flow").asInstanceOf[Double]
    val period = jsonMap("period").asInstanceOf[Double]
    val accuracy = jsonMap("accuracy").asInstanceOf[Double]
    val timestampString = jsonMap("timestamp").asInstanceOf[String]
    val num_lanes = jsonMap("num_lanes").asInstanceOf[Double]

    //Time rounded down to the minute
    val timestamp = getTimeInSeconds(timestampString)

    val splittedKey = key.splitAt(key.lastIndexOf("/"))
    val measurementId = splittedKey._1
    val internalId = splittedKey._2.replace("/", "")
    val keyObservation = measurementId + "/" + internalId + "/" + timestamp

    (
      keyObservation,
      FlowObservation(
        measurementId,
        internalId,
        timestamp,
        latitude,
        longitude,
        flow.toInt,
        period.toInt,
        accuracy.toInt,
        num_lanes.toInt,
        publishTime,
        Instant.now().toEpochMilli
      )
    )
  }

  /**
   * Converts a raw String timestamp to Millis
   *
   * @param timeString String containing the timestamp
   * @return Timestamp in Millis
   */
  def getTimeInSeconds(timeString: String): Long = {
    // Date format of data
    val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.parse(timeString).getTime()
  }

  /**
   * Extracts the timestamp of a raw line (key and observation)
   *
   * @param line line of key and observation
   * @return Timestamp in Millis of observation
   */
  def extractTimestamp(line: String): Long = getTimeInSeconds(line.split("\"timestamp\":\"")(1).substring(0, line.split("\"timestamp\":\"")(1).indexOf("\"")))

  /**
   * Splits a line in a key and a value
   *
   * @param line line to split
   * @return [[(String, String)]] key, observation
   */
  def splitLineInKeyAndValue(line: String): (String, String) = {
    val splittedLine = line.split("=")
    (splittedLine(0), splittedLine(1).replace(" ", ""))
  }


}
