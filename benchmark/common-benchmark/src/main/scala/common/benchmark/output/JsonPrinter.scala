package common.benchmark.output

import common.benchmark.{AggregatableFlowObservation, AggregatableObservation, FlowObservation, RelativeChangeObservation, SpeedObservation}
import io.circe.generic.auto._
import io.circe.syntax._

object JsonPrinter {
    def jsonFor(rawInputRecord: (String, String, String, Long), jobProfile: String): (String, String) = {
        (rawInputRecord._1, s"""{"publishTimestamp":"${rawInputRecord._4.toString}", "jobProfile":"${jobProfile}"}}""")
    }

    def jsonFor(rawInputRecord: (String, String, Long), jobProfile: String): (String, String) = {
        (rawInputRecord._1, s"""{"publishTimestamp":"${rawInputRecord._3.toString}", "jobProfile":"${jobProfile}"}""")
    }

    def jsonFor(obs: FlowObservation): (String, String) = (obs.measurementId, obs.asJson.noSpaces)

    def jsonFor(obs: SpeedObservation): (String, String) = (obs.measurementId, obs.asJson.noSpaces)

    def jsonFor(obs: AggregatableObservation): (String, String) = (obs.measurementId, obs.asJson.noSpaces)

    def jsonFor(obs: AggregatableFlowObservation): (String, String) = (obs.measurementId, obs.asJson.noSpaces)

    def jsonFor(obs: RelativeChangeObservation): (String, String) = (obs.measurementId, obs.asJson.noSpaces)
}