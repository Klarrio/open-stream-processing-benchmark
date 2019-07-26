package common.benchmark.stages

/**
  * Each benchmark tech stack should have an implementation
  * of the following methods,
  * but the type signatures in the different implemenations
  * differ too much to use actual inheritance for enforcing this.
  */
trait InitialStagesTemplate {
  /**
    * Consumes from Kafka from flow topic
    *
    * @return raw kafka stream
    */
//  def readInStreams


  /**
    * Parses data from both streams
    *
    * @return stream of [[common.benchmark.FlowObservation]] and stream of [[common.benchmark.SpeedObservation]]
    */
//  def parseStreams

  /**
    * Joins the flow and speed streams
    *
    * @param parsedFlowStream [[common.benchmark.FlowObservation]]
    * @param parsedSpeedStream [[common.benchmark.SpeedObservation]]
    * @return [[common.benchmark.FlowObservation]] and [[common.benchmark.SpeedObservation]]
    */

//  def joinFlowAndSpeedStream


}
