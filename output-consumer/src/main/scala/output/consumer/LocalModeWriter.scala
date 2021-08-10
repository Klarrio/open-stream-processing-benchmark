package output.consumer

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricFilter, MetricRegistry, Slf4jReporter}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import scala.collection.JavaConverters._

/**
 * For local development!
 * - Reads data from Kafka continuously
 * - Updates latency histogram as data comes in
 * - Sends histograms to Graphite and console logs
 */
object LocalModeWriter {
  def run: Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val registry: MetricRegistry = new MetricRegistry
    val localConfigUtils = new LocalConfigUtils

    val jmxReporter: JmxReporter = JmxReporter.forRegistry(registry).build()
    jmxReporter.start()

    val slf4jReporter: Slf4jReporter = Slf4jReporter.forRegistry(registry)
      .outputTo(logger)
      .convertRatesTo(TimeUnit.MILLISECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS).build()
    slf4jReporter.start(10, TimeUnit.SECONDS)

    if(localConfigUtils.graphiteEnabled) {
      val graphite: Graphite = new Graphite(new InetSocketAddress(localConfigUtils.graphiteHost, localConfigUtils.graphitePort))
      val reporter: GraphiteReporter = GraphiteReporter.forRegistry(registry)
        .prefixedWith("benchmark")
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(graphite)
      reporter.start(1, TimeUnit.SECONDS)
    } else {
      logger.warn("Could not start a connection to Graphite. Will continue with only publishing in logs.")
    }

    //properties of metric consumer
    val props = new Properties()
    props.put("bootstrap.servers", localConfigUtils.kafkaBootstrapServers)
    props.put("group.id", "output-consumer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("auto.offset.reset", "latest")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    //metric consumer
    val consumer = new KafkaConsumer[String, String](props)

    //subscribe to metrics topic
    consumer.subscribe(java.util.Collections.singletonList("metrics"))

    while (true) {
      val metrics = consumer.poll(10000)
      for (metric <- metrics.asScala) {
        val kafkaPublishTimestampResult = metric.timestamp()
        val metricValue = Json.parse(metric.value())
        val publishTimestampInput = (metricValue \ "publishTimestamp").as[Long]
        val jobProfile = (metricValue \ "jobProfile").as[String]
        val durationPublishToPublish = kafkaPublishTimestampResult - publishTimestampInput
        registry.histogram(jobProfile).update(durationPublishToPublish)
      }
    }
  }
}
