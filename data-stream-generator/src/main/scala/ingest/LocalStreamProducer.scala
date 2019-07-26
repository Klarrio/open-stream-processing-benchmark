package ingest

import java.sql.Timestamp
import java.util.Properties
import java.util.concurrent.Executors

import com.codahale.metrics.Meter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

object LocalStreamProducer extends App {
  val logger = LoggerFactory.getLogger(getClass)
  val mode = sys.env("MODE")
  val publishInterval: Long = 1000

  val sparkSession = SparkSession.builder
    .master("local[*]")
    .appName("data-stream-generator")
    .config("spark.driver.memory", "5g")
    .getOrCreate()

  val kafkaProperties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", sys.env("KAFKA_BOOTSTRAP_SERVERS"))
  kafkaProperties.setProperty("linger.ms", "1")
  kafkaProperties.setProperty("batch.size", "8000")

  val flowStats = new Meter()
  val speedStats = new Meter()

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))

  // log some stats every 5 seconds
  private val statsLogger = Future {
    @tailrec def logStats(): Unit = {
      logger.info(f"ingest stats - flow (count:${flowStats.getCount}, rate:${flowStats.getOneMinuteRate}%.1f), speed (count:${speedStats.getCount}, rate:${speedStats.getOneMinuteRate}%.1f)")
      Thread.sleep(5000)
      logStats()
    }

    logStats()
  }
  val fileOrderedDF = sparkSession.sparkContext.textFile("./data/ndw1/time*/*")
    .map { observation =>
      val (keyLine, observationLine) = DataUtils.splitLineInKeyAndValue(observation)
      val timestampOfObservation = DataUtils.extractTimestamp(observation)

      // make the key only the measurement id and put the lane in the msg body to have same partitioning for input and output
      val (measurementId, msg) = DataUtils.putLaneNumberInBody(keyLine, observationLine)
      Observation(timestampOfObservation, measurementId, msg)
    }.groupBy(_.timestamp).sortBy(_._1).collect().toList

  val producer = new KafkaProducer(kafkaProperties, new StringSerializer, new StringSerializer)

  var startTimeBatch = System.currentTimeMillis()
  var startTime100MS = System.currentTimeMillis()
  var startTime5MS = System.currentTimeMillis()

  fileOrderedDF.foreach { groupOfObservations: ((Long, Iterable[Observation])) => // SUPPOSED TO LAST ONE SECOND
    val listOfObservationsOfThisTimestamp = groupOfObservations._2.toList
    val smallGroupsList = listOfObservationsOfThisTimestamp.grouped(2).toList
    if (mode.equals("periodic-burst")) {
      sendWithLargeBursts(groupOfObservations._1, smallGroupsList)
    } else {
      sendAtConstantRate(groupOfObservations._1, smallGroupsList, sys.env("DATA_VOLUME").toInt)
    }
  }

  // Sleep ten minutes before sending the poison pill
  Thread.sleep(600000)

  // Close producer
  producer.close()

  logger.info("END OF FILE")
  Thread.sleep(60000 * 3)

  System.exit(0)

  def sendAtConstantRate(timestamp: Long, smallGroupsList: List[List[Observation]], dataVolume: Int): Unit = {
    0.to(0).foreach { microBatch => //SUPPOSED TO LAST 100 MS
      smallGroupsList.foreach { smallList => //SUPPOSED TO LAST 5 MS
        smallList.foreach { observation =>
          0.to(dataVolume).foreach { volumeIteration =>
            if (observation.message.contains("flow")) {
              flowStats.mark()
              val msg = new ProducerRecord[String, String](
                ConfigUtils.flowTopic,
                microBatch.toString + volumeIteration.toString + observation.key,
                observation.replaceTimestampWithCurrentTimestamp().message
              )
              println("FLOW: " + microBatch.toString + volumeIteration.toString + observation.key + ", " + observation.replaceTimestampWithCurrentTimestamp().message)
              producer.send(msg)
            } else {
              speedStats.mark()
              val msg = new ProducerRecord[String, String](
                ConfigUtils.speedTopic,
                microBatch.toString + volumeIteration.toString + observation.key,
                observation.replaceTimestampWithCurrentTimestamp().message
              )
              println("SPEED: " + microBatch.toString + volumeIteration.toString + observation.key + ", " + observation.replaceTimestampWithCurrentTimestamp().message)
              producer.send(msg)
            }
          }
        }
        val durationOfSendingObservationsFor5ms = System.currentTimeMillis() - startTime5MS
        val sleepingTime5ms = 5 - durationOfSendingObservationsFor5ms
        if (sleepingTime5ms > 0) Thread.sleep(sleepingTime5ms)
        startTime5MS = System.currentTimeMillis()
      }
      val durationOfSendingListFor100ms = System.currentTimeMillis() - startTime100MS
      val sleepingTime = 100 - durationOfSendingListFor100ms
      if (sleepingTime > 0) Thread.sleep(sleepingTime)
      startTime100MS = System.currentTimeMillis()
    }
    logger.info(s"""all observations of timestamp ${new Timestamp(timestamp).toString} sent""")
    val durationOfSendingThisTimestamp = System.currentTimeMillis() - startTimeBatch
    val sleepTimeBeforeNextTimestamp = publishInterval - durationOfSendingThisTimestamp
    if (sleepTimeBeforeNextTimestamp > 0) Thread.sleep(sleepTimeBeforeNextTimestamp)
    startTimeBatch = System.currentTimeMillis()
  }

  def sendWithLargeBursts(timestamp: Long, smallGroupsList: List[List[Observation]]): Unit = {
    val burstNeedsToBeSend = (timestamp % 600000) == 0

    if (burstNeedsToBeSend) {
      logger.info("burst will be send for timestamp " + timestamp)
      sendAtConstantRate(timestamp, smallGroupsList, sys.env("DATA_VOLUME").toInt)
    } else {
      sendAtConstantRate(timestamp, smallGroupsList, 0)
    }
  }
}
