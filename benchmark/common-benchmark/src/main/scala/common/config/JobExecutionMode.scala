package common.config

sealed abstract class JobExecutionMode(val name: String) extends Serializable

object JobExecutionMode {

  case object CONSTANT_RATE extends JobExecutionMode("constant-rate")

  case object LATENCY_CONSTANT_RATE extends JobExecutionMode("latency-constant-rate")

  case object SINGLE_BURST extends JobExecutionMode("single-burst")

  case object PERIODIC_BURST extends JobExecutionMode("periodic-burst")

  case object WORKER_FAILURE extends JobExecutionMode("worker-failure")

  case object MASTER_FAILURE extends JobExecutionMode("master-failure")

  case object FAULTY_EVENT extends JobExecutionMode("faulty-event")

  val values = Seq(CONSTANT_RATE, LATENCY_CONSTANT_RATE, SINGLE_BURST, PERIODIC_BURST, WORKER_FAILURE, MASTER_FAILURE, FAULTY_EVENT)

  def withName(nameOfMode: String): JobExecutionMode = {
    values.find { value => value.name == nameOfMode }.get
  }
}
