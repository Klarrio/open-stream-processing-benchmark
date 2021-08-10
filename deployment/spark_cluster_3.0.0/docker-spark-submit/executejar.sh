#!/bin/bash
set -e

## Defaults
#
: ${SPARK_HOME:?must be set!}
default_opts="--properties-file /spark-defaults.conf"


# Check if CLI args list containes bind address key.
cli_bind_address() {
  echo "$*" | grep -qE -- "--host\b|-h\b|--ip\b|-i\b"
}

# Set permissions on the scratch volumes
scratch_volumes_permissions() {
  mkdir -p $SPARK_HOME/work && chown $SPARK_USER:hadoop $SPARK_HOME/work
  chmod 1777 /tmp
}


## Configuration sourcing
. $SPARK_HOME/sbin/spark-config.sh
. $SPARK_HOME/bin/load-spark-env.sh


## Entrypoint

scratch_volumes_permissions


. $SPARK_HOME/sbin/spark-config.sh
. $SPARK_HOME/bin/load-spark-env.sh

#export SPARK_CORES_MAX=$(($AMT_WORKERS*$WORKER_CPU)) # executors*cores_per_executor + one core for driver
export SPARK_DRIVER_MEMORY="6144m"
export SPARK_DRIVER_CORES=2
#export CONC_GC_THREADS=$(($WORKER_CPU/2))



cd /usr/local/spark/

if [[ "$FRAMEWORK" == "STRUCTUREDSTREAMING" ]]
then
DRIVER_ID=$(./bin/spark-submit --master spark://spark-master.marathon.mesos:7077 \
	--deploy-mode client --supervise \
	--jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.0/spark-sql-kafka-0-10_2.12-3.0.0.jar,https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.0.0/spark-streaming-kafka-0-10_2.12-3.0.0.jar  \
  --driver-memory $SPARK_DRIVER_MEMORY --driver-cores $SPARK_DRIVER_CORES --total-executor-cores $SPARK_CORES_MAX --executor-memory $SPARK_EXECUTOR_MEMORY  \
	--properties-file $SPARK_HOME/spark-defaults.conf \
	--conf spark.LAST_STAGE=$LAST_STAGE \
  --conf spark.KAFKA_AUTO_OFFSET_RESET_STRATEGY=$KAFKA_AUTO_OFFSET_RESET_STRATEGY \
  --conf spark.KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS \
	--conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
	--conf spark.sql.shuffle.partitions=$SPARK_SQL_SHUFFLE_PARTITIONS \
  --conf spark.ACTIVE_HDFS_NAME_NODE=$ACTIVE_HDFS_NAME_NODE \
  --conf spark.METRICS_TOPIC=$TOPICNAME \
  --conf spark.FLOWTOPIC=$FLOWTOPIC \
  --conf spark.SPEEDTOPIC=$SPEEDTOPIC \
	--conf spark.VOLUME=$VOLUME \
	--conf spark.MODE=$MODE \
	--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ParallelGCThreads=$WORKER_CPU -XX:ConcGCThreads=$CONC_GC_THREADS -XX:MaxGCPauseMillis=200 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false​ -Dcom.sun.management.jmxremote.port=8500" \
	--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=200 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false​ -Dcom.sun.management.jmxremote.port=8501" \
	--conf "spark.executorEnv.AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" --conf "spark.executorEnv.AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"\
	--class structuredstreaming.benchmark.StructuredStreamingTrafficAnalyzer \
	$JAR_NAME 0 | grep -o 'driver-\w*-\w*' | head -n1)

elif [[ "$FRAMEWORK" == "SPARK" ]]
then
DRIVER_ID=$(./bin/spark-submit --master spark://spark-master.marathon.mesos:7077 \
	--deploy-mode client --supervise \
	--driver-memory $SPARK_DRIVER_MEMORY --driver-cores $SPARK_DRIVER_CORES --total-executor-cores $SPARK_CORES_MAX --executor-memory $SPARK_EXECUTOR_MEMORY  \
	--jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.0/spark-sql-kafka-0-10_2.12-3.0.0.jar,https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/0.10.2.2/kafka-clients-0.10.2.2.jar,https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.0.0/spark-streaming-kafka-0-10_2.12-3.0.0.jar \
	--properties-file $SPARK_HOME/spark-defaults.conf \
	--conf spark.LAST_STAGE=$LAST_STAGE \
  --conf spark.KAFKA_AUTO_OFFSET_RESET_STRATEGY=$KAFKA_AUTO_OFFSET_RESET_STRATEGY \
  --conf spark.METRICS_TOPIC=$TOPICNAME \
  --conf spark.FLOWTOPIC=$FLOWTOPIC \
  --conf spark.SPEEDTOPIC=$SPEEDTOPIC \
  --conf spark.KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS \
	--conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
	--conf spark.sql.shuffle.partitions=$SPARK_DEFAULT_PARALLELISM \
  --conf spark.ACTIVE_HDFS_NAME_NODE=$ACTIVE_HDFS_NAME_NODE \
	--conf spark.VOLUME=$VOLUME \
	--conf spark.MODE=$MODE \
	--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ParallelGCThreads=$WORKER_CPU -XX:ConcGCThreads=$CONC_GC_THREADS -XX:MaxGCPauseMillis=200 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false​ -Dcom.sun.management.jmxremote.port=8500" \
	--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=200 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false​ -Dcom.sun.management.jmxremote.port=8501" \
	--class spark.benchmark.SparkTrafficAnalyzer \
	$JAR_NAME 0 | grep -o 'driver-\w*-\w*' | head -n1)
fi
