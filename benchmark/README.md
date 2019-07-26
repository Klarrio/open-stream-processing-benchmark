# Open stream processing benchmark

Currently the benchmark includes Apache Spark (Spark Streaming and Structured Streaming), Apache Flink and Kafka Streams.

## Running the benchmark locally

Set the following environment variables:

    export DEPLOYMENT_TYPE=local

Go to the folder benchmark

    cd benchmark

Start sbt

    sbt

Go to the project you want to run:

    project flink-benchmark # other options are spark-benchmark, structured-streaming-benchmark or kafka-benchmark

    run



