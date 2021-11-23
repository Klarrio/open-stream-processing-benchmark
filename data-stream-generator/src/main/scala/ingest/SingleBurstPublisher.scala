package ingest

import java.util.Properties
import java.util.concurrent.Executors

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Utils
import org.apache.spark.sql.SparkSession

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Publishes at a high volume for the first period and then continues at a very low volume.
 *
 * @param sparkSession
 * @param kafkaProperties
 */
class SingleBurstPublisher(sparkSession: SparkSession, kafkaProperties: Properties) extends Publisher {

  override def publish(index: Int) = Future {
    val rawFile = if (ConfigUtils.local) {
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

    val producer = new KafkaProducer(kafkaProperties, new StringSerializer, new StringSerializer)

    // Start publishing when the next round second starts
    val thisSecond = 1000 * Math.round(System.currentTimeMillis() / 1000.0)
    var nextSecond = thisSecond + 1000
    var next100Ms = nextSecond + 100
    var next5MS = nextSecond + 5

    // a high rate is send the first 5 minutes to create the burst,
    // and then we keep sending at a low rate to keep event time progressing but not create load
    // by doing this, later we can see when the framework finished processing all messages
    val startHighRateSending = nextSecond
    val endHighRateSending = nextSecond + 5 * 60 * 1000

    val sleepTimeBeforeStartTimeSecond = nextSecond - System.currentTimeMillis()
    logger.info("will sleep before starting publishing: " + sleepTimeBeforeStartTimeSecond + " millis")
    Thread.sleep(sleepTimeBeforeStartTimeSecond)

    fileOrderedDF.foreach { groupOfObservations: ((Long, Iterable[Observation])) => // SUPPOSED TO LAST ONE SECOND
      // set the values of when the sending should end for this batch
      next5MS = nextSecond + 5
      next100Ms = nextSecond + 100
      nextSecond = nextSecond + 1000
      logger.debug(s"""next second $nextSecond next 100 ms $next100Ms next 5 ms $next5MS""")
      val listOfObservationsOfThisTimestamp: Seq[Observation] = groupOfObservations._2.toList
      val smallGroupsList: Seq[Seq[Observation]] = listOfObservationsOfThisTimestamp.grouped(2).toList

      // If not yet running for 5 minutes than keep sending at high rate
      if (System.currentTimeMillis() < endHighRateSending) {
        sendMessages(ConfigUtils.dataVolume.toInt, listOfObservationsOfThisTimestamp, smallGroupsList)
      } // if already running for more than 5 minutes than just send one flow and speed message per second
      else {
        sendMessages(0, listOfObservationsOfThisTimestamp, smallGroupsList)
      }
    }

    def sendMessages(volume: Int, listOfObservationsOfThisTimestamp: Seq[Observation], smallGroupsList: Seq[Seq[Observation]]): Unit = {
      0.to(9).foreach { microBatch => //SUPPOSED TO LAST 100 MS
        smallGroupsList.foreach { smallList => //SUPPOSED TO LAST 5 MS
          smallList.foreach { observation: Observation =>
            1.to(volume).foreach { volumeIteration =>
              if (observation.message.contains("flow")) {
                flowStats.mark()
                val msg = new ProducerRecord[String, String](
                  ConfigUtils.flowTopic,
                  index + ConfigUtils.publisherNb + microBatch.toString + volumeIteration.toString + observation.key,
                  observation.replaceTimestampWithCurrentTimestamp().message
                )
                producer.send(msg)
              } else {
                if(ConfigUtils.lastStage < 100 ) { // if the stage is equal to or larger than 100 then it needs only one input stream
                  speedStats.mark()
                  val msg = new ProducerRecord[String, String](
                    ConfigUtils.speedTopic,
                    index + ConfigUtils.publisherNb + microBatch.toString + volumeIteration.toString + observation.key,
                    observation.replaceTimestampWithCurrentTimestamp().message
                  )
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
}