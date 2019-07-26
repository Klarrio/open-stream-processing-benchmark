# Data stream generator for open stream processing benchmark

This repository contains code to run a data stream generator for the open stream processing benchmark.

This generator can run in two modes:
- constant-rate: publishes data at a constant rate on the two Kafka topics. 
- periodic-burst: publishes data at a constant rate with periodic bursts every 10 seconds on two Kafka topics.

The original timestamps in the data are replaced by the current time. The generator publishes data at a faster pace (one minute original stream -> one second output stream) than the original stream.

## Running the data stream generator

The following environment variables should be set:

    export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
    export DATA_VOLUME=0 # the inflation factor for the data
    export MODE="constant-rate" # or "periodic-burst"
    export FLOWTOPIC=ndwflow # topic to publish flow events onto
    export SPEEDTOPIC=ndwspeed # topic to publish speed events onto

To run do:

    sbt compile
    sbt run