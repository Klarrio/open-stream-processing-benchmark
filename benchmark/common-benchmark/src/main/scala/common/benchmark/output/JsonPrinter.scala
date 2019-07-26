package common.benchmark.output

import common.benchmark.{AggregatableObservation, FlowObservation, RelativeChangeObservation, SpeedObservation}

object JsonPrinter {
    def jsonFor(rawInputRecord: (String, String, String, Long)): String = s"""{"publishTimestamp":"${rawInputRecord._4.toString}"}"""
    def jsonFor(rawInputRecord: (String, String, Long)): String = s"""{"publishTimestamp":"${rawInputRecord._3.toString}"}"""
    def jsonFor(obs: FlowObservation): String = obs.toJsonString()
    def jsonFor(obs: SpeedObservation): String = obs.toJsonString()
    def jsonFor(obs: AggregatableObservation): String = obs.toJsonString()
    def jsonFor(obs: RelativeChangeObservation): String = obs.toJsonString()
  }