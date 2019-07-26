package common.config

sealed abstract class LastStage(val value: Int) extends Serializable

object LastStage {

  case object UNTIL_INGEST extends LastStage(0)

  case object UNTIL_PARSE extends LastStage(1)

  case object UNTIL_JOIN extends LastStage(2)

  case object UNTIL_TUMBLING_WINDOW extends LastStage(3)

  case object UNTIL_SLIDING_WINDOW extends LastStage(4)

  val values = Seq(UNTIL_INGEST, UNTIL_PARSE, UNTIL_JOIN, UNTIL_TUMBLING_WINDOW, UNTIL_SLIDING_WINDOW)

  def withName(valueOfStage: Int): LastStage = {
    values.find { value => value.value == valueOfStage }.get
  }
}
