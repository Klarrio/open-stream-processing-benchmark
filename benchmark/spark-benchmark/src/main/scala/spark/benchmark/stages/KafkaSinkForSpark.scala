package spark.benchmark.stages

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class KafkaSinkForSpark(producerFactory: () => KafkaProducer[String, String], topic: String) extends Serializable {
  lazy val producerForThisExecutor = producerFactory()

  def send( observation: (String,String)) =
    producerForThisExecutor.send(new ProducerRecord(topic, observation._1, observation._2))

}

object KafkaSinkForSpark {
  def apply(bootstrapServers: String, outputTopic: String): KafkaSinkForSpark = {
    val producerFactory = () => {
      val kafkaProperties = new Properties()
      kafkaProperties.setProperty("bootstrap.servers", bootstrapServers)
      val producer = new KafkaProducer(kafkaProperties, new StringSerializer, new StringSerializer)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSinkForSpark(producerFactory, outputTopic)
  }
}