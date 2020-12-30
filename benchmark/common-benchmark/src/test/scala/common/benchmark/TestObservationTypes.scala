package common.benchmark

import org.scalatest.{Inside, Matchers, WordSpec}

class TestObservationTypes extends WordSpec with Matchers with Inside {
  val firstObservation = AggregatableObservation(
    "u/1/5/r/f/x/4/h/7/f/s/c/PZH01_MST_0690_00",
    List("lane1"),
    1488461880000l,
    52.0265,
    4.68309,
    840,
    60,
    95,
    48,
    95,
    2,
    "jobprofile1"
  )

  //observation 5 minutes ago
  val secondObservation = AggregatableObservation(
    "u/1/5/r/f/x/4/h/7/f/s/c/PZH01_MST_0690_00",
    List("lane2"),
    1488461880000l,
    52.0265,
    4.68309,
    880,
    60,
    95,
    70,
    95,
    2,
    "jobprofile2"
  )

  "Combining aggregated observations" should {
    "result in an aggregated observation" in {
      val aggregatedObservation = firstObservation.combineObservations(secondObservation)

      inside(aggregatedObservation) {
        case agg: AggregatableObservation =>
          agg.measurementId shouldBe "u/1/5/r/f/x/4/h/7/f/s/c/PZH01_MST_0690_00"
          agg.lanes shouldBe List("lane1", "lane2")
          agg.publishTimestamp shouldBe 1488461880000l
          agg.latitude shouldBe 52.0265
          agg.longitude shouldBe 4.68309
          agg.accumulatedFlow shouldBe 840 + 880
          agg.period shouldBe 60
          agg.flowAccuracy shouldBe 95
          agg.averageSpeed shouldBe (48d + 70d) / 2d
          agg.speedAccuracy shouldBe 95
          agg.jobProfile shouldBe "jobprofile1"
      }
    }
  }
}
