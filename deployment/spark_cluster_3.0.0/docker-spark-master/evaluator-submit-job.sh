#!/usr/bin/env bash
export FRAMEWORK=$1
export MODE=$2
export AWS_ACCESS_KEY=$3
export AWS_SECRET_KEY=$4
export LAST_STAGE=$5
export CLUSTER_URL=$6
export INFLUXDB_URL=$7
export DCOS_ACCESS_TOKEN=$8
export FILEPATH=$9
export AMT_WORKERS=${10}
export WORKER_CPU=${11}
export WORKER_MEM=${12}
export BEGINTIME=${13}
export JAR_NAME=${14}
export INPUT_METRICS_PATH=${15}
export RESULTS_PATH=${16}

export SPARK_CORES_MAX=$(($AMT_WORKERS*$WORKER_CPU)) # executors*cores_per_executor + one core for driver
export SPARK_DRIVER_MEMORY="4096m"
export SPARK_DRIVER_CORES=1
export SPARK_DEFAULT_PARALLELISM=$SPARK_CORES_MAX

cd /usr/local/spark/
./bin/spark-submit --master spark://spark-master.marathon.mesos:7077 \
	--deploy-mode cluster \
	--driver-memory $SPARK_DRIVER_MEMORY --driver-cores $SPARK_DRIVER_CORES --total-executor-cores 20 --executor-memory 17408m  \
	--driver-java-options="-Dspark.driver.host=$SPARK_WORKER2_HOST" \
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
	--conf spark.INPUT_METRICS_PATH=$INPUT_METRICS_PATH \
	--conf spark.RESULTS_PATH=$RESULTS_PATH \
	--conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \
	--conf spark.sql.shuffle.partitions=$SPARK_DEFAULT_PARALLELISM \
	--class evaluation.EvaluationMain \
	$JAR_NAME 0
