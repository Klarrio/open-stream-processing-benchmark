# Data Stream Generator for OSPBench

This repository contains code to run a data stream generator for the open stream processing benchmark.

This generator can run in several modes:
- constant rate: publishes data at a constant rate on the two Kafka topics. This mode is used for the following workloads: constant-rate, latency-constant-rate, worker-failure, master-failure
- with periodic bursts: publishes data at a constant rate with periodic bursts every 10 seconds on two Kafka topics.
- with single burst: publishes increased amount of data in the first five minutes of the run and afterwards, a smaller volume.
- with faulty events: publishes a constant rate stream and publishes a faulty event after ten minutes.

The original timestamps in the data are replaced by the current time. The generator publishes data at a faster pace (one minute original stream -> one second output stream) than the original stream.

## Running the data stream generator

The following environment variables should be set:

    S3_ACCESS_KEY=xxx # S3 access key of data input
    S3_SECRET_KEY=xxx # S3 secret key of data input
    KAFKA_BOOTSTRAP_SERVERS=$(hostname -I | head -n1 | awk '{print $1\;}'):9092  # list of Kafka brokers in the form of "host1:port1,host2:port2"
    DATA_VOLUME=0 # inflation factor for the data
    MODE=constant-rate # data characteristics of the input stream (explained further)
    FLOWTOPIC=ndwflow # Kafka topic name for flow data
    SPEEDTOPIC=ndwspeed # Kafka topic name for speed data
    RUNS_LOCAL=true # whether we run locally or on a platform

To run do:

    sbt compile
    sbt run