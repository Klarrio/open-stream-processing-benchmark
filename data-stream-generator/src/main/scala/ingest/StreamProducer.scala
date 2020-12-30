package ingest

import java.util.Properties
import java.util.concurrent.Executors

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

/**
 * Produces a stream on Kafka
 * Four possible configurations:
 * - single-burst: publishes all the messages onto Kafka as quickly as possible
 * - periodic-burst: publishes a load of messages each minute
 * - constant-rate: publishes a constant rate of messages (each 100ms)
 * - faulty-event: publishes a faulty event after a time period to make the job crash
 */
object StreamProducer extends App {
  val logger = LoggerFactory.getLogger(getClass)

  val sparkSession = SparkSession.builder
    .master("local[*]")
    .appName("ndw-publisher")
    .config("spark.driver.memory", "5g")
    .getOrCreate()

  val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
  hadoopConf.set("fs.s3a.endpoint", "s3-eu-central-1.amazonaws.com")
  hadoopConf.set("fs.s3a.access.key", ConfigUtils.s3AccessKey)
  hadoopConf.set("fs.s3a.secret.key", ConfigUtils.s3SecretKey)

  val kafkaProperties = new Properties()
  kafkaProperties.setProperty("bootstrap.servers", ConfigUtils.kafkaBootstrapServers)
  kafkaProperties.setProperty("linger.ms", "20")
  //  kafkaProperties.setProperty("batch.size", "8000")

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))

  val publisherImpl: Publisher = {
    if (ConfigUtils.mode == "single-burst") {
      new SingleBurstPublisher(sparkSession, kafkaProperties)
    } else if (ConfigUtils.mode == "periodic-burst") {
      new PeriodicBurstPublisher(sparkSession, kafkaProperties)
    } else if (ConfigUtils.mode == "constant-rate" || ConfigUtils.mode == "latency-constant-rate" || ConfigUtils.mode == "worker-failure" || ConfigUtils.mode == "master-failure") {
      new ConstantRatePublisher(sparkSession, kafkaProperties)
    } else if (ConfigUtils.mode == "faulty-event") {
      new FaultyEventPublisher(sparkSession, kafkaProperties)
    } else {
      throw new RuntimeException(s"Unsupported app mode ${ConfigUtils.mode}.")
    }
  }
  val ndwPublishers = 0.to(2).map(index => publisherImpl.publish(index: Int))

  // wait for all ingesters to complete
  Await.ready(Future.sequence(ndwPublishers), Duration.Inf)

  logger.info("END OF FILE")
  Thread.sleep(60000 * 3)

  System.exit(0)
}