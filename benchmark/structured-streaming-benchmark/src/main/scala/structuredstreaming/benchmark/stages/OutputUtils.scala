package structuredstreaming.benchmark.stages

import java.sql.Timestamp

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}
import structuredstreaming.benchmark.BenchmarkSettingsForStructuredStreaming

class OutputUtils(sparkSession: SparkSession, settings: BenchmarkSettingsForStructuredStreaming) {
  val timeUDF = udf((time: Timestamp) => time.getTime)

  val r = new scala.util.Random
  val randomLong: Long = r.nextLong()

  def writeToKafka(dataSet: Dataset[_], queryNbr: Int = 0, awaitTermination: Boolean = false): Unit = {
    val kafkaWriter = dataSet
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", settings.general.kafkaBootstrapServers)
      .option("topic", settings.general.outputTopic)
      .option("checkpointLocation", settings.specific.checkpointDir)
      .trigger(settings.specific.trigger)
      .start()
    if (awaitTermination) kafkaWriter.awaitTermination()
  }

  def printToConsole(dataSet: Dataset[_], awaitTermination: Boolean = false): Unit = {
    val consolePrinter = dataSet
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .trigger(settings.specific.trigger)
      .start()

    if (awaitTermination) consolePrinter.awaitTermination()
  }
}

case class KafkaOutputObservation(key: String, value: String)
