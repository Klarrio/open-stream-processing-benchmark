package common.benchmark.output

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class KafkaSinkForSparkAndStorm(producerFactory: () => KafkaProducer[String, String], topic: String, jobProfileKey: String) extends Serializable {
  lazy val producerForThisExecutor = producerFactory()

  def send( observation: String) = producerForThisExecutor.send(new ProducerRecord(topic, jobProfileKey, observation))

}

object KafkaSinkForSparkAndStorm {
  def apply(bootstrapServers: String, outputTopic: String, jobProfileKey: String): KafkaSinkForSparkAndStorm = {
    val producerFactory = () => {
      val kafkaProperties = new Properties()
      kafkaProperties.setProperty("bootstrap.servers", bootstrapServers)
      val producer = new KafkaProducer(kafkaProperties, new StringSerializer, new StringSerializer)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSinkForSparkAndStorm(producerFactory, outputTopic, jobProfileKey: String)
  }
}