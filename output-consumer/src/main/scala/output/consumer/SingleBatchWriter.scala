package output.consumer

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import collection.JavaConverters._

/**
 * For running on a Spark cluster and with JMX exporter!
 * - Reads data from Kafka in one large batch after the run has finished
 * - Extracts the Kafka input and output timestamps from the events
 * - Writes data to S3
 */
object SingleBatchWriter extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass)

  def run = {
    val sparkSession = SparkSession.builder
      .appName("output-consumer")
      .getOrCreate()
    import sparkSession.implicits._

    val configUtils = new ClusterConfigUtils(sparkSession.conf.getAll)

    println(configUtils.toString)

    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.endpoint", configUtils.awsEndpoint)
    hadoopConf.set("fs.s3a.access.key", configUtils.awsAccessKey)
    hadoopConf.set("fs.s3a.secret.key", configUtils.awsSecretKey)

    val inputData = sparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers", configUtils.kafkaBootstrapServers)
      .option("kafka.isolation.level", "read_committed")
      .option("subscribe", configUtils.JOBUUID)
      .option("includeTimestamp", true)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 1000000)
      .load()

    val sample = if (configUtils.mode == "constant-rate" & inputData.count() > 1000000000) {
      inputData.sample(0.01)
    } else if (configUtils.mode == "constant-rate" & inputData.count() > 100000000) {
      inputData.sample(0.1)
    } else if (configUtils.mode == "constant-rate" & inputData.count() > 10000000) {
      inputData.sample(0.1)
    } else inputData

    val schema = schema_of_json(lit(inputData.select("value").as[String].first))
    val timeUDF = udf((time: Timestamp) => time.getTime)

    val inputDataWithTimestampColumn = sample
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .withColumn(
        "inputKafkaTimestamp",
        from_json(col("value"), schema, Map[String, String]().asJava).getItem("publishTimestamp")
      )
      .withColumn(
        "key",
        from_json(col("value"), schema, Map[String, String]().asJava).getItem("jobProfile")
      )
      .select(
        col("key"),
        col("inputKafkaTimestamp"),
        timeUDF(col("timestamp")).as("outputKafkaTimestamp")
      )

    inputDataWithTimestampColumn
      .write
      .json(configUtils.path)

    val metricsStream = sparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers", configUtils.kafkaBootstrapServers)
      .option("subscribe", "metrics-" + configUtils.JOBUUID)
      .option("includeTimestamp", true)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // write gcNotifications
    metricsStream
      .filter(col("key") === "GCNotifications")
      .select("value")
      .write
      .text(configUtils.gcNotificationsPath)

    // write resourceStats
    metricsStream
      .filter(col("key") === "ResourceStats")
      .select("value")
      .write
      .text(configUtils.metricsPath)

    // write cadvisorStats
    metricsStream
      .filter(col("key") === "CadvisorStats")
      .select("value")
      .write
      .text(configUtils.cadvisorPath)

    // write cadvisorHdfsStats
    metricsStream
      .filter(col("key") === "CadvisorHdfsStats")
      .select("value")
      .write
      .text(configUtils.cadvisorHdfsPath)


    // write cadvisorKafkaStats
    metricsStream
      .filter(col("key") === "CadvisorKafkaStats")
      .select("value")
      .write
      .text(configUtils.cadvisorKafkaPath)
  }
}
