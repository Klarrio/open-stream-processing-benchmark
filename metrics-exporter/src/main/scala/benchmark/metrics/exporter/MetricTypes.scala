package benchmark.metrics.exporter

case class ResourceStats(
  containerName: String,
  time: Long,
  nonHeapUsed: Long,
  nonHeapCommitted: Long,
  heapUsed: Long,
  heapCommitted: Long,
  cpuLoad: Double,
  sunProcessCpuLoad: Double,
  javaLangCpu: Double
)

case class GCBeanStats(
  containerName: String,
  time: Long,
  name: String,
  collectionTime: Long,
  collectionCount: Long,
  memoryPoolNames: String,
  lastGcDuration: Option[Long],
  lastGcEndTime: Option[Long],
  lastGcMemoryBefore: Option[Map[String, Long]],
  lastGcMemoryAfter: Option[Map[String, Long]]
)

case class LastGCInformation(
  name: String,
  lastGcDuration: Long,
  lastGcEndTime: Long,
  lastGcMemoryBefore: Map[String, Long],
  lastGcMemoryAfter: Map[String, Long]
)

case class ContainerInfo(
  taskName: String,
  containerId: String,
  cadvisorHost: String
)
