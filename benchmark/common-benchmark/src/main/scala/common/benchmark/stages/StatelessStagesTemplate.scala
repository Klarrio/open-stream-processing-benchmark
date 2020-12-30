package common.benchmark.stages

/**
  * Each benchmark tech stack should have an implementation
  * of the following methods,
  * but the type signatures in the different implemenations
  * differ too much to use actual inheritance for enforcing this.
  */
trait StatelessStagesTemplate {
  /**
    * Consumes from Kafka from flow and speed topic
    *
    * @return raw kafka stream
    */
//  def ingestStage


  /**
    * Parses data from both streams
    *
    * @return stream of [[common.benchmark.FlowObservation]] and stream of [[common.benchmark.SpeedObservation]]
    */
//  def parsingStage

  /**
   * Consumes from Kafka from flow topic
   *
   * @return raw kafka stream
   */
  //  def ingestFlowStreamStage

  /**
   * Parses only flow events
   *
   * @return stream of [[common.benchmark.FlowObservation]]
   */
  //  def parsingFlowStreamStage

}
