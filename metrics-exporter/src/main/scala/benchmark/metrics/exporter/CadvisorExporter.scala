package benchmark.metrics.exporter

import java.util.concurrent.Executors

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source

/**
 * Scrapes the metrics of containers via the cAdvisor REST API and writes them to Kafka
 */
object CadvisorExporter extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

  def scrape(containerInfo: ContainerInfo, kafkaProducer: KafkaProducer[String, String]): Future[Unit] = Future {
    logger.info("Starting cadvisor exporter for id: " + containerInfo.taskName)
    try {
      while (true) {
        val url = "http://" + containerInfo.cadvisorHost + "/api/v2.0/stats/" + containerInfo.containerId
        val msg: String = Source.fromURL(url).mkString
          .replace("{\"" + containerInfo.containerId + "\":", "")
          .dropRight(1)
        val resourceStatsMsg = new ProducerRecord[String, String](ConfigUtils.outputKafkaTopic,
          "CadvisorStats",
          "{\"containerName\":\"" + containerInfo.taskName + "\",\"value\":" + msg + "}")

        kafkaProducer.send(resourceStatsMsg)
        // sleep one minute
        Thread.sleep(60 * 1000)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def scrapeHDFSMetrics(containerInfo: ContainerInfo, kafkaProducer: KafkaProducer[String, String]): Future[Unit] = Future {
    logger.info("Starting cadvisor exporter for id: " + containerInfo.taskName)
    try {
      while (true) {
        val url = "http://" + containerInfo.cadvisorHost + "/api/v2.0/stats/" + containerInfo.containerId
        val msg: String = Source.fromURL(url).mkString
          .replace("{\"" + containerInfo.containerId + "\":", "")
          .dropRight(1)
        val resourceHdfsStatsMsg = new ProducerRecord[String, String](ConfigUtils.outputKafkaTopic,
          "CadvisorHdfsStats",
          "{\"containerName\":\"" + containerInfo.taskName + "\",\"value\":" + msg + "}")

        kafkaProducer.send(resourceHdfsStatsMsg)
        // sleep one minute
        Thread.sleep(60 * 1000)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def scrapeKafkaMetrics(containerInfo: ContainerInfo, kafkaProducer: KafkaProducer[String, String]): Future[Unit] = Future {
    logger.info("Starting cadvisor exporter for id: " + containerInfo.taskName)
    try {
      while (true) {
        val url = "http://" + containerInfo.cadvisorHost + "/api/v2.0/stats/" + containerInfo.containerId
        val msg: String = Source.fromURL(url).mkString
          .replace("{\"" + containerInfo.containerId + "\":", "")
          .dropRight(1)
        val resourceKafkaStatsMsg = new ProducerRecord[String, String](ConfigUtils.outputKafkaTopic,
          "CadvisorKafkaStats",
          "{\"containerName\":\"" + containerInfo.taskName + "\",\"value\":" + msg + "}")

        kafkaProducer.send(resourceKafkaStatsMsg)
        // sleep one minute
        Thread.sleep(60 * 1000)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }
}



