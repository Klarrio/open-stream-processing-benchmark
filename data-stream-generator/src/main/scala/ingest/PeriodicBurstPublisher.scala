package ingest

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession

import scala.concurrent.Future
import scala.util.Try

/**
 * Publishes periodic bursts of data (one file) every time interval.
 *
 * @param sparkSession
 * @param kafkaProperties
 */

class PeriodicBurstPublisher(sparkSession: SparkSession, kafkaProperties: Properties) extends Publisher {
  override def publish(index: Int) = Future {

    val rawFile = if(ConfigUtils.local) {
      logger.info("will read file from local resources")
      sparkSession.sparkContext.textFile(ConfigUtils.localPath)
    } else {
      logger.info("Did not find file locally and will read from S3 instead")
      sparkSession.sparkContext.textFile(ConfigUtils.s3Path)
    }

    val fileOrderedDF = rawFile
      .map { observation =>
        val (keyLine, observationLine) = DataUtils.splitLineInKeyAndValue(observation)
        val timestampOfObservation = DataUtils.extractTimestamp(observation)

        // make the key only the measurement id and put the lane in the msg body to have same partitioning for input and output
        val (measurementId, msg) = DataUtils.putLaneNumberInBody(keyLine, observationLine)
        Observation(timestampOfObservation, measurementId, msg)
      }.groupBy(_.timestamp).sortBy(_._1).collect().toList

    val producer: KafkaProducer[String, String] = new KafkaProducer(kafkaProperties, new StringSerializer, new StringSerializer)


    // Start publishing when the next round second starts
    var thisSecond = 1000 * Math.round(System.currentTimeMillis() / 1000.0)
    var nextSecond = thisSecond + 1000

    val sleepTimeBeforeStartTimeSecond = nextSecond - System.currentTimeMillis()
    logger.info("will sleep before starting publishing: " + sleepTimeBeforeStartTimeSecond + " millis")
    Thread.sleep(sleepTimeBeforeStartTimeSecond)

    fileOrderedDF.foreach { groupOfObservations: ((Long, Iterable[Observation])) => // SUPPOSED TO LAST ONE SECOND
      thisSecond = nextSecond
      nextSecond = nextSecond + 1000
      logger.debug(s"""next second $nextSecond""")

      val listOfObservationsOfThisTimestamp = groupOfObservations._2.toList
      val timestamp = groupOfObservations._1

      // only send burst every 10 seconds => that equals 10 minutes of data so 600 000 milliseconds
      val burstNeedsToBeSend = (timestamp % 600000) == 0

      if (burstNeedsToBeSend) {
        logger.debug("burst will be send for timestamp " + timestamp)

        publishBurst(index, listOfObservationsOfThisTimestamp, producer)
        logger.debug("finished burst for timestamp " + timestamp, thisSecond)
      } else {
        publishAtLowVolumeConstant(index, listOfObservationsOfThisTimestamp, producer, thisSecond)
      }

      // continue at the beginning of the next second

      val sleepTimeBeforeNextSecond = nextSecond - System.currentTimeMillis()
      if (sleepTimeBeforeNextSecond > 0) {
        logger.debug(s"""sleep time $sleepTimeBeforeNextSecond ms before next timestamp $nextSecond ; current time: ${System.currentTimeMillis()}""")
        Thread.sleep(sleepTimeBeforeNextSecond)
      }
    }

    // Close producer
    producer.close()
    logger.info("END OF FILE")
    Thread.sleep(10000000)
  }


  def publishAtLowVolumeConstant(index: Int, listOfObservationsOfThisTimestamp: List[Observation], producer: KafkaProducer[String, String], thisSecond: Long): Unit = {

    var next5MS = thisSecond + 5
    var next100Ms = thisSecond + 100

    val smallGroupsList = listOfObservationsOfThisTimestamp.grouped(2).toList
    0.to(9).foreach { microBatch => //SUPPOSED TO LAST 100 MS
      smallGroupsList.foreach { smallList => //SUPPOSED TO LAST 5 MS
        smallList.foreach { observation =>
          0.to(0).foreach { volumeIteration =>
            if (observation.message.contains("flow")) {
              flowStats.mark()
              val msg = new ProducerRecord[String, String](
                ConfigUtils.flowTopic,
                index + ConfigUtils.publisherNb + microBatch.toString + volumeIteration.toString + observation.key,
                observation.replaceTimestampWithCurrentTimestamp().message)
              producer.send(msg)
            } else {
              if(ConfigUtils.lastStage < 100 ) { // if the stage is larger than 100 then it needs only one input stream
                speedStats.mark()
                val msg = new ProducerRecord[String, String](
                  ConfigUtils.speedTopic,
                  index + ConfigUtils.publisherNb + microBatch.toString + volumeIteration.toString + observation.key,
                  observation.replaceTimestampWithCurrentTimestamp().message)
                producer.send(msg)
              }
            }
          }
        }
        val sleepingTimeTillNext5ms = next5MS - System.currentTimeMillis()
        if (sleepingTimeTillNext5ms > 0) {
          logger.debug(s"""sleep time $sleepingTimeTillNext5ms ms before next 5  ms $next5MS ; current time: ${System.currentTimeMillis()}""")
          Thread.sleep(sleepingTimeTillNext5ms)
        }
        next5MS = next5MS + 5
      }
      val sleepingTimeTillNext100Ms = next100Ms - System.currentTimeMillis()
      if (sleepingTimeTillNext100Ms > 0) {
        logger.debug(s"""sleep time $sleepingTimeTillNext100Ms ms before next 100  ms $next100Ms ; current time: ${System.currentTimeMillis()}""")
        Thread.sleep(sleepingTimeTillNext100Ms)
      }
      next100Ms = next100Ms + 100
    }
  }

  def publishBurst(index: Int, listOfObservationsOfThisTimestamp: List[Observation], producer: KafkaProducer[String, String]): Unit = {

    0.to(9).foreach { microBatch => //SUPPOSED TO LAST 100 MS
      listOfObservationsOfThisTimestamp.foreach { observation =>
        0.to(Math.round(ConfigUtils.dataVolume.toInt/3.0).toInt).foreach { volumeIteration =>
          if (observation.message.contains("flow")) {
            flowStats.mark()
            val msg = new ProducerRecord[String, String](
              ConfigUtils.flowTopic,
              index + ConfigUtils.publisherNb + microBatch.toString + volumeIteration.toString + observation.key,
              observation.replaceTimestampWithCurrentTimestamp().message)
            producer.send(msg)
          } else {
            if(ConfigUtils.lastStage < 100 ) { // if the stage is equal to or larger than 100 then it needs only one input stream
              speedStats.mark()
              val msg = new ProducerRecord[String, String](
                ConfigUtils.speedTopic,
                index + ConfigUtils.publisherNb + microBatch.toString + volumeIteration.toString + observation.key,
                observation.replaceTimestampWithCurrentTimestamp().message)
              producer.send(msg)
            }
          }
        }
      }
    }
  }
}
