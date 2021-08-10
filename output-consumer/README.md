# OSPBench Output Consumer

## Description
This component of OSPBench consumes the output of the benchmark run.
It has two modes. Either it writes the output to S3 (see [SingleBatchWriter](./src/main/scala/output/consumer/SingleBatchWriter.scala)). Or it writes output to the logs and to a Graphite database for visualization in Grafana (see [LocalModeWriter](./src/main/scala/output/consumer/LocalModeWriter.scala).
When running production runs on an AWS cluster, the output has to be written to S3.
The setup with Grafana should only be used for local experimentation.

## Technology
The [LocalModeWriter](./src/main/scala/output/consumer/LocalModeWriter.scala) is pure Scala.
The [SingleBatchWriter](./src/main/scala/output/consumer/SingleBatchWriter.scala) is Scala-Structured Streaming.

## Input
### Processing job events
The events that are read from Kafka need to be in JSON format and have two required fields:
- Field named 'publishTimestamp' which contains the Kafka metadata timestamp of the input on which the event is based. This input timestamp will be used to compute the latency of the event by subtracting the timestamp of the input and the output.
- Field named 'jobProfile' which contains the encoding of the type of experiment that was done and some key settings. It is composed as follows:

        s"${framework}_i${jobId}_e${lastStage}_v${volume}_b${bufferTimeout}_s${shortTermBatchesLookback}_l${longTermBatchesLookback}_t${startTimeOfJob}"

The events should come from Kafka to be able to extract the Kafka timestamp from the metadata to compute the latency.
### JMX metrics (not available in local mode)
In OSPBench, we use a JMX exporter which writes metrics on resource consumption and GC to Kafka.
The type of metric is described by the Kafka key.
The output consumer filters the messages based on the key and writes one file per metric to S3
The Kafka keys that are currently supported are:

1. GCNotifications
2. ResourceStats
3. CadvisorStats
4. CadvisorHdfsStats
5. CadvisorKafkaStats

For more information on the content of these metrics, we refer to the documentation of the JMX exporter.

## Output
The output of this application depends on the mode.

- Local mode: histogram logs in the console and in Graphite
- Cluster mode: 6 files on S3 with metrics in it

## Local mode
In local mode, this component reads all the messages on the metrics topic on Kafka (see [LocalModeWriter](./src/main/scala/output/consumer/LocalModeWriter.scala)).
It reads messages continuously as the local job is running.
It then computes the latency and sends a Dropwizard histogram of the latency to the Graphite database that backs Grafana.

There are some settings that are required to run locally.
You need to adapt the mode in the [resources.conf](./src/main/resources/resources.conf) file: set it to 'local'
Also make sure the Kafka and Grafana hosts and ports are correct in this file.
If you do not want to use Graphite then set the enabled parameter to 'false' in [resources.conf](./src/main/resources/resources.conf). Otherwise, to 'true'.

Then run with:

    cd path/to/output-consumer
    sbt run

## Cluster mode
In cluster mode, this job only runs one time in batch after the benchmark run has finished (see [SingleBatchWriter](./src/main/scala/output/consumer/SingleBatchWriter.scala)).
In this mode, it is supposed to run on a Spark cluster (on DC/OS - AWS).
It reads all the messages on two topics: '$JOBUUID' and 'metrics-$JOBUUID'.
$JOBUUID refers to the UUID that was given to the job at startup.

It writes different types of data to S3:

1. Data on the input and output timestamps of the messages that were published by the stream processing job. It extracts the timestamp fields from the events on the '$JOBUUID' topic and writes that as JSON to S3 on the configured path. The output path is defined in the [ConfigUtils](./src/main/scala/output/consumer/ConfigUtils.scala) class.

   JSON messages in the following format:

        {
            "key": "some-job-profile-key", # Key denoting the characteristics of the job such as which framework, stage, volume, etc.
            "inputKafkaTimestamp": 999999999L, # Timestamp in millis denoting the time the data generator published the event to Kafka
            "outputKafkaTimestamp": 999999999L, # Timestamp in millis denoting the time the processing framework published the event to Kafka
        }

2. GCNotifications: From the events on the 'metrics-$JOBUUID', the GC related messages are filtered out and written to a file on S3. The output path is defined in the [ConfigUtils](./src/main/scala/output/consumer/ConfigUtils.scala) class.
3. ResourceStats: From the events on the 'metrics-$JOBUUID', the resource consumption related messages are filtered out and written to a file on S3. The output path is defined in the [ConfigUtils](./src/main/scala/output/consumer/ConfigUtils.scala) class.
4. CadvisorStats: From the events on the 'metrics-$JOBUUID', the cadvisor messages about the framework containers are filtered out and written to a file on S3. The output path is defined in the [ConfigUtils](./src/main/scala/output/consumer/ConfigUtils.scala) class.
5. CadvisorHdfsStats: From the events on the 'metrics-$JOBUUID', the cadvisor messages about HDFS are filtered out and written to a file on S3. The output path is defined in the [ConfigUtils](./src/main/scala/output/consumer/ConfigUtils.scala) class.
6. CadvisorKafkaStats: From the events on the 'metrics-$JOBUUID', the cadvisor messages about Kafka are filtered out and written to a file on S3. The output path is defined in the [ConfigUtils](./src/main/scala/output/consumer/ConfigUtils.scala) class.

It is important to note that sampling is applied to the '$JOBUUID' topic for very high throughput levels. This can be seen in the code of the SingleBatchWriter.
At least 1 000 000  messages are analyzed.

First, adapt the mode in the [resources.conf](./src/main/resources/resources.conf) file: set it to 'aws'.

To package as a JAR, run:

    cd benchmark/output-consumer
    sbt assembly

Running in cluster mode, requires some extra settings that are inserted via the [Spark Job Configuration when the job is submitted](https://spark.apache.org/docs/latest/configuration.html#dynamically-loading-spark-properties).
The command used to run the job:

    ./bin/spark-submit --master spark://spark-master.marathon.mesos:7077 \
        --deploy-mode cluster \
        --driver-memory 5g --driver-cores 2 --total-executor-cores 20 --executor-memory 17408m  \
        --properties-file /usr/local/spark/conf/spark-defaults.conf \
        --jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar \
        --conf "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4" --conf "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4" \
        --conf spark.FRAMEWORK=$FRAMEWORK  \
        --conf spark.JOBUUID=$JOBUUID   \
        --conf spark.MODE=$MODE \
        --conf spark.KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS  \
        --conf spark.AWS_ACCESS_KEY=$AWS_ACCESS_KEY  \
        --conf spark.AWS_SECRET_KEY=$AWS_SECRET_KEY \
        --conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
        --conf spark.sql.shuffle.partitions=$SPARK_DEFAULT_PARALLELISM \
        --class output.consumer.OutputConsumer \
        $JAR_NAME 0

We usually run on a cluster with 5 workers and 4 CPU - 20 GB per worker. Therefore, the settings for 20 total executor cores and executor memory.
You will need to make sure that the cluster has access to the JAR. You can put the JAR on the Spark master or on some location on S3.

The other environment variables should be set as follows:

- FRAMEWORK: one of the following SPARK/FLINK/KAFKASTREAMS/STRUCTUREDSTREAMING
- JOBUUID: unique identifier of the job
- MODE: the workload which was being executed: constant-rate/latency-constant-rate/single-burst/periodic-burst/worker-failure/master-failure/faulty-event
- KAFKA_BOOTSTRAP_SERVERS: bootstrap servers of the Kafka brokers
- AWS_ACCESS_KEY: access key that has the ability to write to the output path
- AWS_SECRET_KEY: secret key that has the ability to write to the output path
- JAR_NAME: the name of the jar of the output consumer
- SPARK_DEFAULT_PARALLELISM: number of total cores over all executors
