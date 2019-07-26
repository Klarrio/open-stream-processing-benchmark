package flink.benchmark.stages

import flink.benchmark.BenchmarkSettingsForFlink
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}


/**
  * Deserializers for the flow and speed events
  * Returns key, topic and value for each event
  */
class RawObservationDeserializer extends KeyedDeserializationSchema[(String, String, String)] {

  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): (String, String, String) = {
    val key: String = new String(messageKey, "UTF-8")
    val value: String = new String(message, "UTF-8")
    (key, topic, value)
  }

  override def isEndOfStream(t: (String, String, String)): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[(String, String, String)] = { createTypeInformation[(String, String, String)] }

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

