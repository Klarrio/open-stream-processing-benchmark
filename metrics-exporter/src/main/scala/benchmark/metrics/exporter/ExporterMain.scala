package benchmark.metrics.exporter

import java.util.Properties
import java.util.concurrent.Executors

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.Try
import scala.util.parsing.json.JSON

/**
 * Exports JMX and cAdvisor metrics from containers during runs of OSPBench
 */
object ExporterMain {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val kafkaProperties: Properties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", ConfigUtils.kafkaBootstrapServers)

    val producer: KafkaProducer[String, String] = new KafkaProducer(kafkaProperties, new StringSerializer, new StringSerializer)
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

    val jmxScrapers: Seq[Future[Unit]] = jmxHostsOfFrameworkContainers.map { case (id: String, hostIp: String) =>
      JmxExporter.scrape(id, hostIp, producer)
    }.toSeq

    val cadvisorScrapers: Seq[Future[Unit]] = containersToMonitorWithCAdvisor.map { containerInfo: ContainerInfo =>
      CadvisorExporter.scrape(containerInfo, producer)
    }

    val hdfsScrapers: Seq[Future[Unit]] = extractHdfsDataNodes().map { dataNodeContainerInfo =>
      CadvisorExporter.scrapeHDFSMetrics(dataNodeContainerInfo, producer)
    }

    val kafkaScrapers: Seq[Future[Unit]] = kafkaBrokers.map { dataNodeContainerInfo =>
      CadvisorExporter.scrapeKafkaMetrics(dataNodeContainerInfo, producer)
    }

    // wait for all ingesters to complete
    Await.ready(Future.sequence(jmxScrapers ++ cadvisorScrapers ++ hdfsScrapers ++ kafkaScrapers), Duration.Inf)

    logger.info("Stopped sending metrics. System exits.")

    System.exit(0)
  }


  def jmxHostsOfFrameworkContainers: Array[(String, String)] = {
    // Parse the host names that are in the environment variables in the following way: "job-manager:10.0.3.222,task-manager-1:10.0.3.221"
    ConfigUtils.jmxHosts.split(",")
      .map { host =>
        val id = host.split(":")(0)
        val ip = host.split(":")(1)
        (id, ip)
      }
  }


  def containersToMonitorWithCAdvisor: Seq[ContainerInfo] = {
    val client = new DefaultHttpClient
    val containers = ConfigUtils.cadvisorHosts.split(",")
      .toList
      .flatMap { cadvisorHost =>
        val url = "http://" + cadvisorHost + "/api/v2.0/spec/?type=docker&recursive=true"
        val result = Source.fromURL(url).mkString

        val parsedResult = JSON.parseFull(result).get.asInstanceOf[Map[String, Any]]
          .filter { dockerContainerInfo: (String, Any) =>
            val applicationName = Try(dockerContainerInfo._2.asInstanceOf[Map[String, Any]]("labels").asInstanceOf[Map[String, Any]]("application")
              .toString).getOrElse("")
            (applicationName.contains("spark") || applicationName.contains("flink") || applicationName.contains("kafka-thread"))
          }
          .map { dockerContainerInfo =>
            val applicationName = dockerContainerInfo._2.asInstanceOf[Map[String, Any]]("labels")
              .asInstanceOf[Map[String, Any]]("application")
              .toString

            ContainerInfo(applicationName, dockerContainerInfo._1, cadvisorHost)
          }
        parsedResult
      }.distinct
    client.getConnectionManager.shutdown()

    logger.info(containers.toString())
    containers
  }

  def kafkaBrokers: Seq[ContainerInfo] = {
    val client = new DefaultHttpClient
    val containers = ConfigUtils.kafkaBootstrapServers.split(",")
      .toList
      .flatMap { kafkaHost =>
        val cadvisorHost = kafkaHost.replace(":10000", "") + ":8888"
        val url = "http://" + cadvisorHost + "/api/v2.0/spec/?type=docker&recursive=true"
        val result = Source.fromURL(url).mkString

        val parsedResult = JSON.parseFull(result).get.asInstanceOf[Map[String, Any]]
          .filter { dockerContainerInfo: (String, Any) =>
            val imageName = Try(dockerContainerInfo._2.asInstanceOf[Map[String, Any]]("image").asInstanceOf[String])
              .toOption.getOrElse("")
            imageName.contains("kafka-cluster")
          }
          .map { dockerContainerInfo =>
            val applicationName = dockerContainerInfo._2.asInstanceOf[Map[String, Any]]("labels")
              .asInstanceOf[Map[String, Any]]("MESOS_TASK_ID")
              .toString

            ContainerInfo(applicationName, dockerContainerInfo._1, cadvisorHost)
          }
        parsedResult
      }.distinct
    client.getConnectionManager.shutdown()

    logger.info(containers.toString())
    containers
  }


  // Returns a map containing the frameworks info of Mesos
  // From this map the container names will be extracted
  private def requestMesosTaskHistories(): Map[String, Any] = {
    val url = ConfigUtils.clusterUrl + "/mesos/frameworks"
    val httpGet = new HttpGet(url)

    // set the desired header values
    // To get token:
    // dcos config show core.dcos_acs_token
    httpGet.setHeader("Authorization", "token=" + ConfigUtils.dcosAccessToken)
    // execute the request
    val client = new DefaultHttpClient
    val response = client.execute(httpGet)

    val in = response.getEntity
    val encoding = response.getEntity.getContentEncoding

    val result = EntityUtils.toString(in, "UTF-8")
    client.getConnectionManager.shutdown()

    val parsedResult = JSON.parseFull(result).get.asInstanceOf[Map[String, Any]]
    logger.info(result.toString)
    parsedResult
  }

  def extractHdfsDataNodes(): List[ContainerInfo] = {
    val parsedResult = requestMesosTaskHistories()
    val hdfs = parsedResult("frameworks")
      .asInstanceOf[List[Map[String, Any]]]
      .find { taskJson =>
        taskJson("name").toString.contains("hdfs")
      }.get

    val dataNodes = hdfs("tasks").asInstanceOf[List[Map[String, Any]]]
      .filter {
        taskMap =>
          taskMap("name").asInstanceOf[String].contains("data")
      }
    logger.info(dataNodes.toString)

    val containerInfoDataNodes = dataNodes.map { dataNode =>
      val taskName = dataNode("name").asInstanceOf[String]

      val containerId = dataNode("statuses").asInstanceOf[List[Map[String, Any]]].head("container_status")
        .asInstanceOf[Map[String, Any]]("container_id")
        .asInstanceOf[Map[String, Any]]("parent")
        .asInstanceOf[Map[String, Any]]("value").toString

      val cadvisorHost = dataNode("labels").asInstanceOf[List[Map[String, Any]]]
        .find { label => label("key").asInstanceOf[String].contains("offer_hostname") }.get
      val cadvisorHostWithPort = cadvisorHost("value").asInstanceOf[String] + ":8888"

      ContainerInfo(taskName, "/mesos/" + containerId, cadvisorHostWithPort)
    }
    logger.info(containerInfoDataNodes.toString)
    containerInfoDataNodes
  }
}