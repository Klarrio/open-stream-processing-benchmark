#! /bin/bash
export FRAMEWORK=$1
export MODE=$2
export JOBUUID=$3
export AMT_WORKERS=${4:-5}
export WORKER_CPU=${5:-4}
export WORKER_MEM=${6:-20}
export AWS_ACCESS_KEY=`cat AWS_ACCESS_KEY`
export AWS_SECRET_KEY=`cat AWS_SECRET_KEY`
export JAR_PATH=`cat benchmark-jars-path`
export JAR_NAME=$JAR_PATH/benchmark-output-consumer-assembly-3.1.jar; echo "- JAR_NAME = $JAR_NAME"
export OUTPUT_METRICS_PATH=`cat benchmark-metrics-path`

# Requesting the values for the required environment variables for ndw publisher and output consumer
## get the Kafka brokers
BOOTSTRAP_SERVER_LIST=($(dcos task kafka-brokers | awk '{ print $2 }' | grep 10))
BROKER_LIST_STRING="${BOOTSTRAP_SERVER_LIST[*]}"
export KAFKA_BOOTSTRAP_SERVERS=$(echo "${BROKER_LIST_STRING//${IFS:0:1}/,}" | sed -E "s/([^,]+)/\1:10000/g")
echo "Kafka bootstrap servers configured as: $KAFKA_BOOTSTRAP_SERVERS"

export SPARK_MASTER_IP_ADDR=$(dcos task | grep spark-master | awk '{print $2}')
echo "SPARK_MASTER_IP_ADDR=$SPARK_MASTER_IP_ADDR"
export SPARK_MASTER_DOCKER_ID=$(ssh -oStrictHostKeyChecking=no core@$SPARK_MASTER_IP_ADDR docker ps | grep spark-master | awk '{print $1}')
echo "SPARK_MASTER_DOCKER_ID=$SPARK_MASTER_DOCKER_ID"

#driver host
export SPARK_WORKER2_HOST=$(dcos task | grep spark-worker-2 | awk '{print $2}')

echo "starting output consumer for $JOBUUID"
# Submit output consumer
export OUTPUT_CONSUMER_SUBMIT_JOB_CMD="(./output-consumer-submit-job.sh $FRAMEWORK $MODE $KAFKA_BOOTSTRAP_SERVERS $AWS_ACCESS_KEY $AWS_SECRET_KEY $JOBUUID $SPARK_WORKER2_HOST $AMT_WORKERS $WORKER_CPU $WORKER_MEM $JAR_NAME $OUTPUT_METRICS_PATH)"
echo $OUTPUT_CONSUMER_SUBMIT_JOB_CMD
ssh -oStrictHostKeyChecking=no core@$SPARK_MASTER_IP_ADDR docker exec -i $SPARK_MASTER_DOCKER_ID 'bash -c "'"$OUTPUT_CONSUMER_SUBMIT_JOB_CMD"'"'

# stay in this script while the output consumer is running
sleep 1m
ACTIVE_DRIVER_ID=$(ssh -oStrictHostKeyChecking=no  core@$SPARK_MASTER_IP_ADDR docker exec -i $SPARK_MASTER_DOCKER_ID 'bash -c "'"(./active-driver-check.sh $SPARK_MASTER_IP_ADDR)"'"')

k=1
while [[ $ACTIVE_DRIVER_ID == *"driver"* ]];
do
    sleep 1m
    k=$(( $k + 1 ))
    if ! ((k % 5)); then
        echo "output consumer running for $k minutes for $JOBUUID"
    fi
    ACTIVE_DRIVER_ID=$(ssh -oStrictHostKeyChecking=no  core@$SPARK_MASTER_IP_ADDR docker exec -i $SPARK_MASTER_DOCKER_ID 'bash -c "'"(./active-driver-check.sh $SPARK_MASTER_IP_ADDR)"'"')
done

echo "output consumer finished in $k minutes"
