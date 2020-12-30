package kafka.benchmark.stages

import common.benchmark.{FlowObservation, SpeedObservation}
import org.scalatest.{Inside, Matchers, WordSpec}

class SerdeTest extends WordSpec with Matchers with Inside {

  "serializing and deserializing " should {
    "result in the same speed observation" in {
      val speedObs = SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584240000l, 52.431985, 4.64708, 95.0, 95, 2, "")

      val speedSerializer = new SpeedSerializer
      val speedDeserializer = new SpeedDeserializer

      val serializedSpeedObs = speedSerializer.serialize("topic-1", speedObs)
      val deserializedSpeedObs = speedDeserializer.deserialize("topic-1", serializedSpeedObs)

      inside(deserializedSpeedObs) {
        case speed: SpeedObservation =>
          speed.measurementId should be("GEO02_PNHTI532r")
          speed.internalId should be("lane1")
          speed.publishTimestamp should be(1489584240000l)
          speed.latitude should be(52.431985)
          speed.longitude should be(4.64708)
          speed.speed should be(95.0)
          speed.accuracy should be(95)
          speed.numLanes should be(2)
      }
    }


    "result in the same flow observation" in {
      val flowObs = FlowObservation("GEO02_PNHTI532r", "lane1", 1489584240000l, 52.431985, 4.64708, 180, 60, 95, 2, "")

      val flowSerializer = new FlowSerializer
      val flowDeserializer = new FlowDeserializer

      val serializedFlowObs = flowSerializer.serialize("topic-1", flowObs)
      val deserializedFlowObs = flowDeserializer.deserialize("topic-1", serializedFlowObs)

      println(flowObs)
      println(deserializedFlowObs)

      inside(deserializedFlowObs) {
        case flow: FlowObservation =>
          flow.measurementId should be("GEO02_PNHTI532r")
          flow.internalId should be("lane1")
          flow.publishTimestamp should be(1489584240000l)
          flow.latitude should be(52.431985)
          flow.longitude should be(4.64708)
          flow.flow should be(180)
          flow.period should be(60)
          flow.accuracy should be(95)
          flow.numLanes should be(2)
      }
    }
  }
}
