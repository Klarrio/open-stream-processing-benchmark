package benchmark.metrics.exporter

object ConfigUtils {
  val framework: String = sys.env("FRAMEWORK")

  val kafkaBootstrapServers: String = sys.env("KAFKA_BOOTSTRAP_SERVERS")
  val outputKafkaTopic: String = "metrics-" + sys.env("TOPICNAME")

  val jmxHosts: String = sys.env("JMX_HOSTS")

  val cadvisorHosts: String = sys.env("CADVISOR_HOSTS")

  val clusterUrl: String = sys.env("CLUSTER_URL")
  val dcosAccessToken: String = sys.env("DCOS_ACCESS_TOKEN")
}
