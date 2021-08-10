package evaluation.utils

/**
 * Data Types for evaluation purposes.
 */

case class RunConfiguration(
  framework: String,
  phase: Int,
  scale: Int, //volume inflation factor
  bufferTimeout: Int, //linger time of kafka
  shortLb: Int, // short lookback
  longLb: Int, // long lookback,
  startTime: Long
)

case class MetricObservation(
  runConfiguration: RunConfiguration,
  inputKafkaTimestamp: Long,
  outputKafkaTimestamp: Long
)

case class QueryResponse(
  containerId: String,
  taskName: String,
  runConfig: RunConfiguration,
  queryResponse: Map[String, Any]
)

case class CpuUsage(
  containerName: String,
  containerTaskId: String,
  runConfiguration: RunConfiguration,
  time: Long,
  cpuUsage: Float,
  cpuUsagePct: Double
)

case class MemUsage(
  containerName: String,
  runConfiguration: RunConfiguration,
  time: Long,
  memUsageMB: Double
)

case class BytesTransferred(
  containerName: String,
  runConfiguration: RunConfiguration,
  time: Long,
  mbits: Double
)


case class PgFaults(
  containerName: String,
  runConfiguration: RunConfiguration,
  time: Long,
  pgFaults: Double
)

case class PacketsDropped(
  containerName: String,
  runConfiguration: RunConfiguration,
  time: Long,
  dropped: Long
)



case class OperationTime(
  containerName: String,
  runConfiguration: RunConfiguration,
  time: Long,
  operationTime: Long
)