package kafka.benchmark.stages

import common.benchmark._
import common.benchmark.output._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{TimeWindowedDeserializer, TimeWindowedSerializer, Windowed}

object CustomObjectSerdes {
  //Serializers and Deserializers for Kafka
  implicit val StringSerde: Serde[String] = Serdes.String()
  implicit val FlowObservationSerde: Serde[FlowObservation] = Serdes.serdeFrom(new FlowSerializer, new FlowDeserializer)
  implicit val SpeedObservationSerde: Serde[SpeedObservation] = Serdes.serdeFrom(new SpeedSerializer, new SpeedDeserializer)
  implicit val AggregatableObservationSerde: Serde[AggregatableObservation] = Serdes.serdeFrom(new AggregatableObservationSerializer, new AggregatableObservationDeserializer)
  implicit val AggregatedObservationListSerde: Serde[List[AggregatableObservation]] = Serdes.serdeFrom(new AggregregationListSerializer, new AggregationListDeserializer)
  implicit val RelativeChangeObservationSerde: Serde[RelativeChangeObservation] = Serdes.serdeFrom(new RelativeChangeSerializer, new RelativeChangeDeserializer)
  implicit val WindowedStringSerde: Serde[Windowed[String]] = Serdes.serdeFrom(new TimeWindowedSerializer[String](), new TimeWindowedDeserializer[String]())
}
