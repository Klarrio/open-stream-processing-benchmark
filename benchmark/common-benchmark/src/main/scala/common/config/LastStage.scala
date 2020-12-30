package common.config

sealed abstract class LastStage(val value: Int) extends Serializable

object LastStage {

  // Complex pipeline

  case object UNTIL_INGEST extends LastStage(0)

  case object UNTIL_PARSE extends LastStage(1)

  case object UNTIL_JOIN extends LastStage(2)

  case object UNTIL_TUMBLING_WINDOW extends LastStage(3)

  case object UNTIL_SLIDING_WINDOW extends LastStage(4)

  case object UNTIL_LOWLEVEL_TUMBLING_WINDOW extends LastStage(5)

  case object UNTIL_LOWLEVEL_SLIDING_WINDOW extends LastStage(6)


  // Simple stateful pipelines

  case object REDUCE_WINDOW_WITHOUT_JOIN extends LastStage(100)

  case object NON_INCREMENTAL_WINDOW_WITHOUT_JOIN extends LastStage(101)

  val values = Seq(
    // complex pipelines
    UNTIL_INGEST,
    UNTIL_PARSE,
    UNTIL_JOIN,
    UNTIL_TUMBLING_WINDOW,
    UNTIL_SLIDING_WINDOW,
    UNTIL_LOWLEVEL_TUMBLING_WINDOW,
    UNTIL_LOWLEVEL_SLIDING_WINDOW,

    // simple stateful pipelines
    REDUCE_WINDOW_WITHOUT_JOIN,
    NON_INCREMENTAL_WINDOW_WITHOUT_JOIN
  )

  def withName(valueOfStage: Int): LastStage = {
    values.find { value => value.value == valueOfStage }.get
  }
}
