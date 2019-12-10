package flink.benchmark.stages

import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord


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

  override def getProducedType: TypeInformation[(String, String, Long)] = { createTypeInformation[(String, String, Long)] }

}


/**
  * Serializer for Kafka messages
  *
  * @param settings
  */
class OutputMessageSerializer(settings: BenchmarkSettingsForFlink) extends KeyedSerializationSchema[String]{
  override def serializeKey(element: String): Array[Byte] = {
    settings.specific.jobProfileKey.getBytes
  }


  override def serializeValue(element: String): Array[Byte] = {
    element.getBytes
  }


  override def getTargetTopic(element: String): String = {
    settings.general.outputTopic
  }
}

