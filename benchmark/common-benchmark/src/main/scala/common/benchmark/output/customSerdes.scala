package common.benchmark.output


import java.util
import java.util.Objects

import common.benchmark._
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json._

class FlowSerializer extends Serializer[FlowObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override final def serialize(topic: String, data: FlowObservation): Array[Byte] =
    if (Objects.isNull(data)) null else data.toJsonString().getBytes()
}

class FlowDeserializer extends Deserializer[FlowObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): FlowObservation = {
    implicit val newFlowObservationReader: Reads[FlowObservation] = Json.reads[FlowObservation]
    val newFlowObservationJson = Json.parse(new String(data))
    Json.fromJson[FlowObservation](newFlowObservationJson).get
  }
}

object FlowJsonDeserializer {
  def fromJsonString(str: String): FlowObservation = {
    implicit val newFlowObservationReader: Reads[FlowObservation] = Json.reads[FlowObservation]
    val newFlowObservationJson = Json.parse(str)
    Json.fromJson[FlowObservation](newFlowObservationJson).get
  }
}

class SpeedSerializer extends Serializer[SpeedObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: SpeedObservation): Array[Byte] = {
    if (Objects.isNull(data)) null else data.toJsonString().getBytes()
  }
}

class SpeedDeserializer extends Deserializer[SpeedObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): SpeedObservation = {
    implicit val newSpeedObservationReader: Reads[SpeedObservation] = Json.reads[SpeedObservation]
    val newSpeedObservationJson = Json.parse(new String(data))
    Json.fromJson[SpeedObservation](newSpeedObservationJson).get
  }
}

object SpeedJsonDeserializer {
  def fromJsonString(str: String): SpeedObservation = {
    implicit val newSpeedObservationReader: Reads[SpeedObservation] = Json.reads[SpeedObservation]
    val newSpeedObservationJson = Json.parse(str)
    Json.fromJson[SpeedObservation](newSpeedObservationJson).get
  }
}

class AggregatableObservationSerializer extends Serializer[AggregatableObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: AggregatableObservation): Array[Byte] = {
    if (Objects.isNull(data)) null else data.toJsonString().getBytes()
  }
}

class AggregatableObservationDeserializer extends Deserializer[AggregatableObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): AggregatableObservation = {
    implicit val aggObservationReader: Reads[AggregatableObservation] = Json.reads[AggregatableObservation]
    val AggObservationJson = Json.parse(new String(data))
    Json.fromJson[AggregatableObservation](AggObservationJson).get
  }
}

class AggregregationListSerializer extends Serializer[List[AggregatableObservation]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: List[AggregatableObservation]): Array[Byte] = {
    if (Objects.isNull(data)) null else data.map(_.toJsonString()).mkString("[", ",", "]").getBytes()
  }
}

class AggregationListDeserializer extends Deserializer[List[AggregatableObservation]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): List[AggregatableObservation] = {
    implicit val aggObservationReader: Reads[AggregatableObservation] = Json.reads[AggregatableObservation]
    val aggObservationJson = Json.parse(new String(data))
    Json.fromJson[List[AggregatableObservation]](aggObservationJson).get
  }
}

class RelativeChangeSerializer extends Serializer[RelativeChangeObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: RelativeChangeObservation): Array[Byte] = {
    if (Objects.isNull(data)) null else data.toJsonString().getBytes()
  }
}

class RelativeChangeDeserializer extends Deserializer[RelativeChangeObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): RelativeChangeObservation = {
    Json.parse(new String(data)).as[RelativeChangeObservation](RelativeChangeObservationtoJSONHelpers.relativeChangeObservationReader)
  }
}


