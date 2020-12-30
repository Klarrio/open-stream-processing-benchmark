package flink.benchmark.testutils

import common.benchmark.{AggregatableFlowObservation, AggregatableObservation, RelativeChangeObservation}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

// create a testing sink
class AggregatableObservationCollectSink extends SinkFunction[AggregatableObservation] {

  override def invoke(value: AggregatableObservation, context: SinkFunction.Context[_]): Unit = {
    synchronized {
      AggregatableObservationCollectSink.values.add(value)
    }
  }
}

object AggregatableObservationCollectSink {
  // must be static
  val values: java.util.List[AggregatableObservation] = new java.util.ArrayList()
}

// create a testing sink
class AggregatableFlowCollectSink extends SinkFunction[AggregatableFlowObservation] {

  override def invoke(value: AggregatableFlowObservation, context: SinkFunction.Context[_]): Unit = {
    synchronized {
      AggregatableFlowCollectSink.values.add(value)
    }
  }
}

object AggregatableFlowCollectSink {
  // must be static
  val values: java.util.List[AggregatableFlowObservation] = new java.util.ArrayList()
}

// create a testing sink
class RelativeChangeCollectSink extends SinkFunction[RelativeChangeObservation] {

  override def invoke(value: RelativeChangeObservation, context: SinkFunction.Context[_]): Unit = {
    synchronized {
      RelativeChangeCollectSink.values.add(value)
    }
  }
}

object RelativeChangeCollectSink {
  // must be static
  val values: java.util.List[RelativeChangeObservation] = new java.util.ArrayList()
}