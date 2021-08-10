#!/usr/bin/env bash
export FRAMEWORK=$1
export MODE=$2
export KAFKA_BOOTSTRAP_SERVERS=$3
export AWS_ACCESS_KEY=$4
export AWS_SECRET_KEY=$5
export JOBUUID=$6
export SPARK_WORKER2_HOST=$7
export AMT_WORKERS=${8}
export WORKER_CPU=${9}
export WORKER_MEM=${10}
export JAR_NAME=${11}
export OUTPUT_METRICS_PATH=${12}

export SPARK_CORES_MAX=$(($AMT_WORKERS*$WORKER_CPU)) # executors*cores_per_executor + one core for driver
export SPARK_DRIVER_MEMORY="4096m"
export SPARK_DRIVER_CORES=1
export SPARK_DEFAULT_PARALLELISM=$SPARK_CORES_MAX

cd /usr/local/spark/
DRIVER_ID=$(./bin/spark-submit --master spark://spark-master.marathon.mesos:7077 \
	--deploy-mode cluster \
	--driver-memory $SPARK_DRIVER_MEMORY --driver-cores $SPARK_DRIVER_CORES --total-executor-cores 20 --executor-memory 17408m  \
	--driver-java-options="-Dspark.driver.host=$SPARK_WORKER2_HOST" \
	--properties-file /usr/local/spark/conf/spark-defaults.conf \
	--jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar \
	--conf "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4" --conf "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4" \
	--conf spark.FRAMEWORK=$FRAMEWORK  \
	--conf spark.JOBUUID=$JOBUUID   \
	--conf spark.MODE=$MODE \
	--conf spark.KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS  \
	--conf spark.AWS_ACCESS_KEY=$AWS_ACCESS_KEY  \
	--conf spark.AWS_SECRET_KEY=$AWS_SECRET_KEY \
	--conf spark.OUTPUT_METRICS_PATH=$OUTPUT_METRICS_PATH \
	--conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
	--conf spark.sql.shuffle.partitions=$SPARK_DEFAULT_PARALLELISM \
	--conf spark.driver.host=$SPARK_WORKER2_HOST \
	--class output.consumer.OutputConsumer \
	$JAR_NAME 0 | grep -o 'driver-\w*-\w*' | head -n1)


echo $DRIVER_ID > /driver-output-consumer.txt
