package benchmark.metrics.exporter

import java.lang.management.{MemoryMXBean, OperatingSystemMXBean}
import java.util.concurrent.Executors

import com.sun.management.GarbageCollectorMXBean
import io.circe.generic.auto._
import javax.management.MBeanServerConnection
import javax.management.remote.{JMXConnectorFactory, JMXServiceURL}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory

import io.circe.syntax._
import javax.management.ObjectName

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Collects resource statistics and GC statistics via JMX and publishes them to Kafka
 */
object JmxExporter extends Serializable {
  val logger = LoggerFactory.getLogger(getClass)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

  val youngGenerationGarbageCollector = Seq(
    "Copy",
    "PS Scavenge",
    "ParNew",
    "G1 Young Generation"
  )
  val oldGenerationGarbageCollector = Seq(
    "MarkSweepCompact",
    "PS MarkSweep",
    "ConcurrentMarkSweep",
    "G1 Old Generation"
  )

  val names = Seq(
    "MinorGCCount",
    "MinorGCTime",
    "MajorGCCount",
    "MajorGCTime"
  )

  def scrape(id: String, jmxHost: String, kafkaProducer: KafkaProducer[String, String]): Future[Unit] = Future {
    try {
      logger.info("Starting JMX exporter for id: " + id)
      val jmxUrl1 = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi".format(jmxHost, "8500"))
      val jmxUrl2 = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi".format(jmxHost, "8501"))
      val jmxc = Try(JMXConnectorFactory.connect(jmxUrl1, null)).getOrElse(JMXConnectorFactory.connect(jmxUrl2, null))
      val connection: MBeanServerConnection = jmxc.getMBeanServerConnection
      var time = System.currentTimeMillis()

      // Memory proxy
      val memProxy = ManagementFactory.newPlatformMXBeanProxy(connection, ManagementFactory.MEMORY_MXBEAN_NAME,
        classOf[MemoryMXBean])

      // CPU proxies
      val sunCpuProxy: com.sun.management.OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
      val cpuProxy = ManagementFactory.newPlatformMXBeanProxy(connection, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME,
        classOf[OperatingSystemMXBean])

      while (true) {

        val resourceStats = ResourceStats(
          id,
          System.currentTimeMillis(),
          memProxy.getNonHeapMemoryUsage.getUsed,
          memProxy.getNonHeapMemoryUsage.getCommitted,
          memProxy.getHeapMemoryUsage.getUsed,
          memProxy.getHeapMemoryUsage.getCommitted,
          sunCpuProxy.getSystemCpuLoad,
          sunCpuProxy.getProcessCpuLoad,
          cpuProxy.getSystemLoadAverage
        )


        val resourceStatsMsg = new ProducerRecord[String, String](ConfigUtils.outputKafkaTopic,
          "ResourceStats",
          resourceStats.asJson.noSpaces)
        kafkaProducer.send(resourceStatsMsg)

        val gcName = new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*")
        connection.queryNames(gcName, null).toList.foreach { name: ObjectName =>
          val gcMxBean: GarbageCollectorMXBean = ManagementFactory.newPlatformMXBeanProxy(connection, name.getCanonicalName, classOf[GarbageCollectorMXBean])
          val javaGcMxBean: java.lang.management.GarbageCollectorMXBean = ManagementFactory.newPlatformMXBeanProxy(connection, name.getCanonicalName, classOf[java.lang.management.GarbageCollectorMXBean])

          val gcStats = GCBeanStats(
            id,
            System.currentTimeMillis(),
            gcMxBean.getName,
            javaGcMxBean.getCollectionTime,
            javaGcMxBean.getCollectionCount,
            gcMxBean.getMemoryPoolNames.mkString("/"),
            // every second we send only the last GC info... This is enough proximation of the GC cycle times
            Try(gcMxBean.getLastGcInfo.getDuration).toOption,
            Try(gcMxBean.getLastGcInfo.getEndTime).toOption,
            Try(gcMxBean.getLastGcInfo.getMemoryUsageBeforeGc.mapValues(_.getUsed).toMap).toOption,
            Try(gcMxBean.getLastGcInfo.getMemoryUsageAfterGc.mapValues(_.getUsed).toMap).toOption
          )
          val gcStatsMsg = new ProducerRecord[String, String](ConfigUtils.outputKafkaTopic,
            "GCNotifications",
            gcStats.asJson.noSpaces)
          kafkaProducer.send(gcStatsMsg)
        }

        val processingTime = System.currentTimeMillis() - time
        if (processingTime < 1000) {
          Thread.sleep(1000 - processingTime) //scrape metrics every second
        }
        time = System.currentTimeMillis()
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }
}