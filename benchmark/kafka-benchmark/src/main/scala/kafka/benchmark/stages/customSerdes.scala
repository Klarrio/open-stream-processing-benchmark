package kafka.benchmark.stages

import java.util
import java.util.Objects

import common.benchmark._
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.syntax._

class FlowSerializer extends Serializer[FlowObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override final def serialize(topic: String, data: FlowObservation): Array[Byte] =
    if (Objects.isNull(data)) null else data.asJson.noSpaces.getBytes()
}

class FlowDeserializer extends Deserializer[FlowObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): FlowObservation = {
    parser.decode[FlowObservation](new String(data)).right.get
  }
}

class SpeedSerializer extends Serializer[SpeedObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: SpeedObservation): Array[Byte] = {
    if (Objects.isNull(data)) null else data.asJson.noSpaces.getBytes()
  }
}

class SpeedDeserializer extends Deserializer[SpeedObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): SpeedObservation = {
    parser.decode[SpeedObservation](new String(data)).right.get
  }
}


class AggregatableObservationSerializer extends Serializer[AggregatableObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: AggregatableObservation): Array[Byte] = {
    if (Objects.isNull(data)) null else data.asJson.noSpaces.getBytes()
  }
}

class AggregatableObservationDeserializer extends Deserializer[AggregatableObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): AggregatableObservation = {
    parser.decode[AggregatableObservation](new String(data)).right.get
  }
}


class AggregatableFlowSerializer extends Serializer[AggregatableFlowObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: AggregatableFlowObservation): Array[Byte] = {
    if (Objects.isNull(data)) null else data.asJson.noSpaces.getBytes()
  }
}

class AggregatableFlowDeserializer extends Deserializer[AggregatableFlowObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): AggregatableFlowObservation = {
    parser.decode[AggregatableFlowObservation](new String(data)).right.get
  }
}

class AggregatableFlowListSerializer extends Serializer[List[AggregatableFlowObservation]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: List[AggregatableFlowObservation]): Array[Byte] = {
    if (Objects.isNull(data)) null else data.map(_.asJson.noSpaces).mkString("[", ",", "]").getBytes()
  }
}

class AggregatableFlowListDeserializer extends Deserializer[List[AggregatableFlowObservation]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): List[AggregatableFlowObservation] = {
    parser.decode[List[AggregatableFlowObservation]](new String(data)).right.get
  }
}

class AggregationListSerializer extends Serializer[List[AggregatableObservation]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: List[AggregatableObservation]): Array[Byte] = {
    if (Objects.isNull(data)) null else data.map(_.asJson.noSpaces).mkString("[", ",", "]").getBytes()
  }
}

class AggregationListDeserializer extends Deserializer[List[AggregatableObservation]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): List[AggregatableObservation] = {
    parser.decode[List[AggregatableObservation]](new String(data)).right.get
  }
}

class RelativeChangeSerializer extends Serializer[RelativeChangeObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serialize(topic: String, data: RelativeChangeObservation): Array[Byte] = {
    if (Objects.isNull(data)) null else data.asJson.noSpaces.getBytes()
  }
}

class RelativeChangeDeserializer extends Deserializer[RelativeChangeObservation] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): RelativeChangeObservation = {
    parser.decode[RelativeChangeObservation](new String(data)).right.get
  }
}

//
//object RelativeChangeObservationtoJSONHelpers {
//  implicit val aggregatedObservationReader: Reads[AggregatableObservation] = Json.reads[AggregatableObservation]
//  implicit val relativeChangePercentagesReader: Reads[RelativeChangePercentages] = Json.reads[RelativeChangePercentages]
//  implicit val relativeChangeObservationReader: Reads[RelativeChangeObservation] = Json.reads[RelativeChangeObservation]
//
//  implicit val aggregatedObservationWriter: OWrites[AggregatableObservation] = Json.writes[AggregatableObservation]
//  implicit val relativeChangePercentagesWriter: Writes[RelativeChangePercentages] = Json.writes[RelativeChangePercentages]
//  implicit val relativeChangeObservationWriter: Writes[RelativeChangeObservation] = Json.writes[RelativeChangeObservation]
//}
