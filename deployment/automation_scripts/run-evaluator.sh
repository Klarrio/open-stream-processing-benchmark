#! /bin/bash
export FRAMEWORK=$1
export MODE=$2
export LAST_STAGE=$3
export FILEPATH=$4
export BEGINTIME=$5
export ENDTIME=$6
export AMT_WORKERS=${7:-5}
export WORKER_CPU=${8:-4}
export WORKER_MEM=${9:-20}
export SPARK_EXECUTOR_MEMORY=${10:-"17g"} # usually "17408m"
export AWS_ACCESS_KEY=`cat AWS_ACCESS_KEY`
export AWS_SECRET_KEY=`cat AWS_SECRET_KEY`
export JAR_PATH=`cat benchmark-jars-path`
export JAR_NAME=$JAR_PATH/stream-processing-evaluator-assembly-4.1.jar; echo "- JAR_NAME = $JAR_NAME"
export INPUT_METRICS_PATH=`cat benchmark-metrics-path`
export BASIS_PATH=`cat benchmark-results-path`
BEGINTIME_FMT=${BEGINTIME//:/_}
export RESULTS_PATH="$BASIS_PATH/$FRAMEWORK/$MODE/stage$LAST_STAGE/${AMT_WORKERS}x-${WORKER_CPU}cpu-${WORKER_MEM}gb/${BEGINTIME_FMT}-${FILEPATH}/network"
export AWS_CLOUDWATCH_OUTPUT_PATH=s3$(echo $RESULTS_PATH | cut -c 4-) # we need the path without s3a but with s3 at the beginning


export SPARK_MASTER_IP_ADDR=$(dcos task | grep spark-master | awk '{print $2}')
echo "SPARK_MASTER_IP_ADDR=$SPARK_MASTER_IP_ADDR"
export SPARK_MASTER_DOCKER_ID=$(ssh -oStrictHostKeyChecking=no core@$SPARK_MASTER_IP_ADDR docker ps | grep spark-master | awk '{print $1}')
echo "SPARK_MASTER_DOCKER_ID=$SPARK_MASTER_DOCKER_ID"

#driver host
export SPARK_WORKER2_HOST=$(dcos task | grep spark-worker-2 | awk '{print $2}')

# DCOS IP (for evaluator)
ssh-add ~/.ssh/id_rsa_benchmark
DCOS_DNS_ADDRESS=$(aws cloudformation describe-stacks --region eu-west-1 --stack-name=streaming-benchmark | jq '.Stacks[0].Outputs | .[] | select(.Description=="Master") | .OutputValue' |  awk '{print tolower($0)}')
export CLUSTER_URL=http://${DCOS_DNS_ADDRESS//\"}
echo $CLUSTER_URL

# InfluxDB (for evaluator)
INFLUXDB_NODE=$(dcos task influxdb | awk '{ print $2 }' | grep 10)
export INFLUXDB_URL=http://${INFLUXDB_NODE}:8086
echo $INFLUXDB_URL

# DCOS access token (for evaluator)
export DCOS_ACCESS_TOKEN=$(dcos config show core.dcos_acs_token)
echo $DCOS_ACCESS_TOKEN


echo "will now run"
echo "(./evaluator-submit-job.sh $FRAMEWORK $MODE $AWS_ACCESS_KEY $AWS_SECRET_KEY $LAST_STAGE $CLUSTER_URL $INFLUXDB_URL $DCOS_ACCESS_TOKEN $FILEPATH $AMT_WORKERS $WORKER_CPU $WORKER_MEM $BEGINTIME_FMT $JAR_NAME $INPUT_METRICS_PATH $BASIS_PATH)"
export SUBMIT_JOB_CMD="(./evaluator-submit-job.sh $FRAMEWORK $MODE $AWS_ACCESS_KEY $AWS_SECRET_KEY $LAST_STAGE $CLUSTER_URL $INFLUXDB_URL $DCOS_ACCESS_TOKEN $FILEPATH $AMT_WORKERS $WORKER_CPU $WORKER_MEM $BEGINTIME_FMT $JAR_NAME $INPUT_METRICS_PATH $BASIS_PATH)"
ssh -oStrictHostKeyChecking=no core@$SPARK_MASTER_IP_ADDR docker exec -i $SPARK_MASTER_DOCKER_ID 'bash -c "'"$SUBMIT_JOB_CMD"'"'

# stay in this script while the output consumer is running
sleep 1m
ACTIVE_DRIVER_ID=$(ssh -oStrictHostKeyChecking=no  core@$SPARK_MASTER_IP_ADDR docker exec -i $SPARK_MASTER_DOCKER_ID 'bash -c "'"(./active-driver-check.sh $SPARK_MASTER_IP_ADDR)"'"')


#export the network metrics of the run
AWS_INSTANCES_WITH_ID=$(aws ec2 describe-instances --region eu-west-1 --filter "Name=instance-type,Values=m5n.4xlarge,m5n.8xlarge" --query "Reservations[*].Instances[*].[InstanceId]" --output text)
for instance_id in $AWS_INSTANCES_WITH_ID
  do
  aws cloudwatch get-metric-statistics --metric-name NetworkIn --region eu-west-1 --start-time $BEGINTIME --end-time $ENDTIME --period 60 --statistics Maximum --namespace AWS/EC2 --dimensions Name=InstanceId,Value=$instance_id | jq '.["Datapoints"]' |  aws s3 cp - "$AWS_CLOUDWATCH_OUTPUT_PATH/${instance_id}_networkin_maximum.json"
  aws cloudwatch get-metric-statistics --metric-name NetworkIn --region eu-west-1 --start-time $BEGINTIME --end-time $ENDTIME --period 60 --statistics Average --namespace AWS/EC2 --dimensions Name=InstanceId,Value=$instance_id | jq '.["Datapoints"]' | aws s3 cp - "$AWS_CLOUDWATCH_OUTPUT_PATH/${instance_id}_networkin_average.json"
  aws cloudwatch get-metric-statistics --metric-name NetworkOut --region eu-west-1 --start-time $BEGINTIME --end-time $ENDTIME --period 60 --statistics Maximum --namespace AWS/EC2 --dimensions Name=InstanceId,Value=$instance_id | jq '.["Datapoints"]' | aws s3 cp - "$AWS_CLOUDWATCH_OUTPUT_PATH/${instance_id}_networkout_maximum.json"
  aws cloudwatch get-metric-statistics --metric-name NetworkOut --region eu-west-1 --start-time $BEGINTIME --end-time $ENDTIME --period 60 --statistics Average --namespace AWS/EC2 --dimensions Name=InstanceId,Value=$instance_id | jq '.["Datapoints"]' | aws s3 cp - "$AWS_CLOUDWATCH_OUTPUT_PATH/${instance_id}_networkout_average.json"
  done

k=1
while [[ $ACTIVE_DRIVER_ID == *"driver"* ]];
do
    sleep 1m
    k=$(( $k + 1 ))
    if ! ((k % 5)); then
        echo "evaluator running for $k minutes for $FILEPATH"
    fi
    ACTIVE_DRIVER_ID=$(ssh -oStrictHostKeyChecking=no  core@$SPARK_MASTER_IP_ADDR docker exec -i $SPARK_MASTER_DOCKER_ID 'bash -c "'"(./active-driver-check.sh $SPARK_MASTER_IP_ADDR)"'"')
done

echo "evaluator finished in $k minutes"
