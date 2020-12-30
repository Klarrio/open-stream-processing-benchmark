package flink.benchmark.stages

import java.lang

import common.benchmark.AggregatableObservation
import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import io.circe.generic.auto._
import io.circe.syntax._

/**
 * Deserializers for the flow and speed events
 * Returns key, topic and value for each event
 */
class RawObservationDeserializer extends KafkaDeserializationSchema[(String, String, Long)] {

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String, Long) = {
    val key: String = new String(record.key(), "UTF-8")
    val value: String = new String(record.value(), "UTF-8")
    (key, value, record.timestamp())
  }


  override def isEndOfStream(t: (String, String, Long)): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[(String, String, Long)] = {createTypeInformation[(String, String, Long)]}

}

/**
 * Serializer for Kafka messages
 *
 * @param settings
 */
class OutputMessageSerializer(settings: BenchmarkSettingsForFlink) extends KafkaSerializationSchema[(String, String)] {

  override def serialize(element: (String, String), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord(settings.general.outputTopic, element._1.getBytes(), element._2.getBytes())
  }
}


