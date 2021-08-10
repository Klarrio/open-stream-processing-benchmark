# OSPBench Metrics Evaluation Suite

This component of OSPBench consumes the output that was written to S3 by the output consumer.
It then computes aggregations and metrics on this data.
This component generates reports on the metrics of the benchmark run and preprocesses data for visualization by Jupyter notebooks.

## Technology
This is an Apache Spark batch job in Scala. It should run on a Spark cluster for large amounts of data.

## Input
This component needs several input files in order to compute all metrics:

#### 1. Events processed by the stream processing frameworks
The output consumer wrote a sample of these to S3.

JSON messages in the following format:

    {
        "key": "some-job-profile-key", # Key denoting the characteristics of the job such as which framework, stage, volume, etc.
        "inputKafkaTimestamp": 999999999L, # Timestamp in millis denoting the time the data generator published the event to Kafka 
        "outputKafkaTimestamp": 999999999L, # Timestamp in millis denoting the time the processing framework published the event to Kafka 
    }


#### 2. cAdvisor data
It sends requests to InfluxDB where this data is stored.

It requests:
- cpu_usage_user
- cpu_user_total
- cpu_usage_system
- memory_usage
- memory_swap
- memory_cache
- memory_pgfault
- memory_pgmajfault
- disk_io_wait
- fs_io_wait
- fs_weighted_io_time
- fs_write_time
- fs_read_time
- rx_bytes
- tx_bytes
- rx_dropped
- tx_dropped

Metrics on the resource utilization of HDFS and Kafka were stored on S3 by the JMX exporter.
The JMX exporter used the cAdvisor API V2 to get these metrics.

#### 3. Metrics from the JMX exporter
These were also written to S3 by the output consumer.

One file contains resource metrics in the following format:

    {
        "containerName": "some-container-tag", # String tag of container
        "time": 9999999999L, # timestamp in millis 
        "nonHeapUsed": 9999999999L, # off heap memory usage in bytes
        "nonHeapCommitted": 9999999999L, # off heap memory committed in bytes
        "heapUsed": 9999999999L, # heap memory usage in bytes
        "heapCommitted": 9999999999L, # heap memory committed in bytes
        "cpuLoad": 0.5, # com.sun.management.OperatingSystemMXBean systemCpuLoad
        "sunProcessCpuLoad": 0.5, # com.sun.management.OperatingSystemMXBean processCpuLoad
        "javaLangCpu": 0.5 # java.lang.management.ManagementFactory OperatingSystemMXBean System Load Average
    }

The other file contains GC stats in the following format:

    {
        "containerName": "some-container-tag", # String tag of container
        "time": 9999999999L, # timestamp in millis 
        "name": "name-of-memory-manager", 
        "collectionTime": 1234L, # approximate accumulated collection elapsed time in milliseconds.
        "collectionCount": 123L, # total number of collections that have occurred. 
        "memoryPoolNames": "pool1,pool2", # names of memory pools that this memory manager manages as a comma-separated list
        "lastGcDuration": Option[Long], # duration of last gc cycle in millis
        "lastGcEndTime": Option[Long], # end time of last gc cycle in millis
        "lastGcMemoryBefore": Option[Map[String, Long]], # size of each memory pool before last GC cycle in bytes
        "lastGcMemoryAfter": Option[Map[String, Long]] # size of each memory pool after last GC cycle in bytes
    }

## Output
The computed metrics are written to S3 in CSV format. They can either serve as inputs for the notebooks or as reports on a specific run.
The number and content of the files depends on the mode of the run.

In general, there are files on:
- general report on statistics over the entire run (e.g. median latency, average CPU utilization,...)
- latency timeseries: for each second of the run a set of percentiles of the latency of the events that were published to Kafka in that second.
- throughput: input and output throughput over time
- cpu utilization per container timeseries (total, user, system)
- memory utilization per container timeseries (heap)
- network utilization per container timeseries

## Running the Evaluation Suite
Before assembling the JAR some configurations need to be set in the [resources.conf](./src/main/resources/resources.conf) file.

- AWS endpoint
- Data input path: path where the job can find the input data
- Data ouput path: path where the job can place the outputs

Create the JAR of the program by running:

    cd path/to/evaluator
    sbt assembly

Remove certs:

    zip -d stream-processing-evaluator-assembly-0.3.jar 'META-INF/*.SF' 'META-INF/*.RSA'

The command used to run the job on the Spark cluster is:

    ./bin/spark-submit --master spark://spark-master.marathon.mesos:7077 \
        --deploy-mode cluster \
        --driver-memory 5g --driver-cores 2 --total-executor-cores 20 --executor-memory 17408m  \
        --properties-file /usr/local/spark/conf/spark-defaults.conf \
        --jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar \
        --conf "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4" --conf "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4" \
        --conf spark.FRAMEWORK=$FRAMEWORK  \
        --conf spark.MODE=$MODE \
        --conf spark.AWS_ACCESS_KEY=$AWS_ACCESS_KEY  \
        --conf spark.AWS_SECRET_KEY=$AWS_SECRET_KEY \
        --conf spark.FILEPATH=$FILEPATH \
        --conf spark.LAST_STAGE=$LAST_STAGE \
        --conf spark.CLUSTER_URL=$CLUSTER_URL  \
        --conf spark.INFLUXDB_URL=$INFLUXDB_URL  \
        --conf spark.AMT_WORKERS=$AMT_WORKERS \
        --conf spark.WORKER_CPU=$WORKER_CPU \
        --conf spark.WORKER_MEM=$WORKER_MEM \
        --conf spark.DCOS_ACCESS_TOKEN=$DCOS_ACCESS_TOKEN \
        --conf spark.BEGINTIME=$BEGINTIME \
        --conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
        --conf spark.sql.shuffle.partitions=$SPARK_DEFAULT_PARALLELISM \
        --class evaluation.EvaluationMain \
        $JAR_NAME 0

We usually run on a cluster with 5 workers and 4 CPU - 20 GB per worker. Therefore, the settings for 20 total executor cores and executor memory.
You will need to make sure that the cluster has access to the JAR. You can put the JAR on the Spark master or on some location on S3.

The other environment variables should be set as follows:

- FRAMEWORK: one of the following SPARK/FLINK/KAFKASTREAMS/STRUCTUREDSTREAMING
- MODE: the workload which was being executed: constant-rate/latency-constant-rate/single-burst/periodic-burst/worker-failure/master-failure/faulty-event
- AWS_ACCESS_KEY: access key that has the ability to write to the output path
- AWS_SECRET_KEY: secret key that has the ability to write to the output path
- JAR_NAME: the name of the jar of the output consumer
- SPARK_DEFAULT_PARALLELISM: number of total cores over all executors
- LAST_STAGE: index of the pipeline that will be executed
- CLUSTER_URL: url of the DC/OS cluster
- INFLUXDB_URL: url of influx DB to get the cadvisor data
- DCOS_ACCESS_TOKEN: token to access DC/OS to scrape the marathon services
- FILEPATH: JOBUUID of the run you want to analyze
- AMT_WORKERS: number of workers that were running during the benchmark run
- WORKER_CPU: number of CPUs per worker that were running during the benchmark run
- WORKER_MEM: GBs of memory per worker that were running during the benchmark run
- BEGINTIME: time at which the run started