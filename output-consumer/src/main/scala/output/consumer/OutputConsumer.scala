package output.consumer

/**
 * Starting point of the application
 * Can run in two modes:
 * - Local mode with histograms in Graphite/Grafana and logs
 * - Cluster mode with writing to S3
 */
object OutputConsumer {
  def main(args: Array[String]): Unit = {
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    val configUtils = new ConfigUtils

    if (configUtils.local) {
      LocalModeWriter.run
    } else {
      SingleBatchWriter.run
    }
  }
}
