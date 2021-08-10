#! /bin/bash
Help()
{
   # Display Help
   echo "Flink latency measurement workload"
   echo
   echo "The pipelines that need to be tested need to be defined in the stages variable in the script."
   echo
   echo "Parameters:"
   echo "-h    Print this Help."
   echo
}
# Get the options
while getopts "h" option; do
   case $option in
      h) # display Help
          Help
          exit;;
   esac
done

# hardcoded env vars
echo "Job Configuration:"
export FRAMEWORK="FLINK"; echo "- FRAMEWORK = $FRAMEWORK"
export MODE="latency-constant-rate"; echo "- MODE = $MODE"
export JOB_VERSION=3.0; echo "- JOB_VERSION = $JOB_VERSION"
export DATA_VOLUME=0; echo "- DATA_VOLUME = $DATA_VOLUME"
export FLOWTOPIC=ndwflow; echo "- FLOWTOPIC = $FLOWTOPIC"
export SPEEDTOPIC=ndwspeed; echo "- SPEEDTOPIC = $SPEEDTOPIC"
export PUBLISHER_COUNT=1
export KAFKA_AUTO_OFFSET_RESET_STRATEGY="latest"
export BUFFER_TIMEOUT=0
export INPUT_DATA_PATH=`cat ../benchmark-input-data-path`
export JAR_BUCKET_NAME=`cat ../benchmark-jars-bucket`
export JAR_PATH=`cat ../benchmark-jars-path`
stages=$(seq 0 4); echo "STAGES:"; echo "${stages[@]}"

export AMT_WORKERS=5; echo "- AMT_WORKERS = $AMT_WORKERS"
export WORKER_CPU=4; echo "- WORKER_CPU = $WORKER_CPU"
export WORKER_MEM=20; echo "- WORKER_MEM = $WORKER_MEM"
export NUM_PARTITIONS=20; echo "- NUM_PARTITIONS = $NUM_PARTITIONS"

export AWS_ACCESS_KEY=`cat ../AWS_ACCESS_KEY`
export AWS_SECRET_KEY=`cat ../AWS_SECRET_KEY`
if [ -z "$AWS_ACCESS_KEY" ] || [ -z "$AWS_SECRET_KEY" ] ; then
        echo 'Missing AWS_ACCESS_KEY and/or AWS_SECRET_KEY. Fill it in in the AWS_ACCESS_KEY and AWS_SECRET_KEY files in the automation_scripts folder.' >&2
        exit 1
fi
echo
echo

eval $(ssh-agent -s)
ssh-add ~/.ssh/id_rsa_benchmark

# start up Flink cluster
cd ..
./start-flink-cluster.sh $AMT_WORKERS $WORKER_CPU
cd ../aws_marathon_files

# get the hosts of the jmx containers
FLINK_CONTAINER_NAME=($(dcos task flink | awk '{ print $1 }' | grep flink))
FLINK_CONTAINER_IP=($(dcos task flink | awk '{ print $2 }' | grep 10))
export JMX_HOSTS=""
for i in $(seq 0 $AMT_WORKERS)
do
  JMX_HOSTS="${JMX_HOSTS}${FLINK_CONTAINER_NAME[$i]}:${FLINK_CONTAINER_IP[$i]},"
done
JMX_HOSTS=${JMX_HOSTS::-1} # remove the last comma
echo $JMX_HOSTS
echo "cadvisor hosts of flink containers"
export CADVISOR_HOSTS=""
for i in $(seq 0 $AMT_WORKERS)
do
  CADVISOR_HOSTS="${CADVISOR_HOSTS}${FLINK_CONTAINER_IP[$i]}:8888,"
done
CADVISOR_HOSTS=${CADVISOR_HOSTS::-1} # remove the last comma
echo $CADVISOR_HOSTS

#### HDFS name nodes
HDFS_NAME_NODE_1=$(dcos task name-0-node | awk '{ print $2 }' | grep 10)
HDFS_NAME_NODE_2=$(dcos task name-1-node | awk '{ print $2 }' | grep 10)
STATE_NAME_NODE_1=$(curl "http://$HDFS_NAME_NODE_1:9002/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" | jq  '.beans[0].State')
STATE_NAME_NODE_2=$(curl "http://$HDFS_NAME_NODE_2:9002/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" | jq  '.beans[0].State')
if [[ $STATE_NAME_NODE_1 == *"active"* ]]; then
   export ACTIVE_HDFS_NAME_NODE="$HDFS_NAME_NODE_1:9001"
else
   export ACTIVE_HDFS_NAME_NODE="$HDFS_NAME_NODE_2:9001"
fi

# Requesting the values for the required environment variables for ndw publisher and output consumer
# get the Kafka brokers
BOOTSTRAP_SERVER_LIST=($(dcos task kafka-brokers | awk '{ print $2 }' | grep 10))
BROKER_LIST_STRING="${BOOTSTRAP_SERVER_LIST[*]}"
export KAFKA_BOOTSTRAP_SERVERS=$(echo "${BROKER_LIST_STRING//${IFS:0:1}/,}" | sed -E "s/([^,]+)/\1:10000/g")


# DCOS IP (for jmx exporter)
DCOS_DNS_ADDRESS=$(aws cloudformation describe-stacks --region eu-west-1 --stack-name=streaming-benchmark | jq '.Stacks[0].Outputs | .[] | select(.Description=="Master") | .OutputValue' |  awk '{print tolower($0)}')
export CLUSTER_URL=http://${DCOS_DNS_ADDRESS//\"}
echo $CLUSTER_URL

# DCOS access token (for jmx exporter)
export DCOS_ACCESS_TOKEN=$(dcos config show core.dcos_acs_token)
echo $DCOS_ACCESS_TOKEN

aws s3 cp s3://$JAR_BUCKET_NAME/flink-benchmark-assembly-$JOB_VERSION.jar  ~/benchmark-jars/

topicnames=()
begintimes=()
endtimes=()

for LAST_STAGE in $stages
do
  export LAST_STAGE=$LAST_STAGE
  cd ../aws_marathon_files

  export VOLUME_PER_PUBLISHER=$((($DATA_VOLUME+($PUBLISHER_COUNT-1))/$PUBLISHER_COUNT))
  echo "adding publishers"
  for PUBLISHER_NB in $(seq 1 $PUBLISHER_COUNT)
  do
    export PUBLISHER_NB=$PUBLISHER_NB
    envsubst < aws-publisher-with-env.json > aws-publisher-without-env-$PUBLISHER_NB.json
    dcos marathon app add aws-publisher-without-env-$PUBLISHER_NB.json
  done

  ################## RUN PREPARATION ######################
  # Create a new topic for the metrics of this job
  # Do this by generating a UUID and using this as the topicname for the Kafka metrics topic
  # as well as the output filename for the output consumer and evaluator
  export TOPICNAME=$(uuidgen)
  cd ../automation_scripts
  ./create-kafka-topic.sh $TOPICNAME $NUM_PARTITIONS
  # Add the topic name to a list that will be used later on to start an output consumer and evaluator per topic
  topicnames+=("$TOPICNAME")
  begintime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "begintime of $TOPICNAME - $begintime"
  begintimes+=($begintime)
  sleep 10
  cd ../aws_marathon_files
  echo "topic $TOPICNAME created"

  # Start up the jmx metrics gathering
  echo "Start up JMX metrics exporter"
  envsubst < jmx-exporter-with-env.json > jmx-exporter-without-env.json
  dcos marathon app add jmx-exporter-without-env.json

	# set name of JAR
  export JAR_NAME_STRING="flink-benchmark-$LAST_STAGE-assembly-$JOB_VERSION.jar"
	echo $JAR_NAME_STRING
	JAR_NAME=flink-benchmark-$LAST_STAGE-assembly-$JOB_VERSION.jar

  JAR_PATH_FOR_JOB_MANAGER=$JAR_PATH/$JAR_NAME

	# replace the last stage variable in benchmarkConfig.conf by the new value for i
	envsubst < aws_with_env.conf > aws.conf
	# put this new benchmarkConfig.conf file in the flink JAR
	jar -uf ~/benchmark-jars/flink-benchmark-assembly-$JOB_VERSION.jar aws.conf
	cp -b  ~/benchmark-jars/flink-benchmark-assembly-$JOB_VERSION.jar $JAR_NAME

	# upload JAR to flink cluster
	echo "Uploading JAR to Flink for stage $LAST_STAGE"
  aws s3 cp $JAR_NAME s3://$JAR_BUCKET_NAME --acl public-read

  export FLINK_JOBMANAGER_IP_ADDR=$(dcos task flink-jobmanager | awk '{print $2}' | grep 10.)
  echo "FLINK_JOBMANAGER_IP_ADDR=$FLINK_JOBMANAGER_IP_ADDR"
  export FLINK_JOBMANAGER_DOCKER_ID=$(ssh -oStrictHostKeyChecking=no core@$FLINK_JOBMANAGER_IP_ADDR docker ps | grep flink-jobmanager | awk '{print $1}')
  echo "FLINK_JOBMANAGER_DOCKER_ID=$FLINK_JOBMANAGER_DOCKER_ID"

  ssh core@$FLINK_JOBMANAGER_IP_ADDR docker exec -i $FLINK_JOBMANAGER_DOCKER_ID 'bash -c "./submit-job.sh '"$JAR_PATH_FOR_JOB_MANAGER"'"'

	##################  BENCHMARK RUN ######################
	# let warm up finish for two minutes
	sleep 2m


	# start constant rate benchmarking publisher
	echo "Starting ndw-publisher for stage $LAST_STAGE"
  for PUBLISHER_NB in $(seq 1 $PUBLISHER_COUNT)
  do
    export PUBLISHER_NB=$PUBLISHER_NB
    dcos marathon app start /benchmark/$MODE-publisher-$PUBLISHER_NB
  done

	# wait 31 min
	for k in {1..31}
	do
	    sleep 1m
	    echo "benchmark running for $k minutes"
	done



	##################  END OF RUN ######################
	# kill metrics-exporter
	echo "Killing jmx-exporter for stage $LAST_STAGE"
	dcos marathon app stop jmx-exporter
  dcos marathon app remove jmx-exporter

	# kill streaming job
	echo "Killing streaming job for stage $LAST_STAGE"
  ssh core@$FLINK_JOBMANAGER_IP_ADDR docker exec -d $FLINK_JOBMANAGER_DOCKER_ID 'bash -c "./cancel-job.sh"'

  endtime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "endtime of $TOPICNAME - $endtime"
  endtimes+=($endtime)

  # remove publisher
	echo "Killing ndw-publisher for stage $LAST_STAGE"
  for PUBLISHER_NB in $(seq 1 $PUBLISHER_COUNT)
  do
    export PUBLISHER_NB=$PUBLISHER_NB
    dcos marathon app stop /benchmark/$MODE-publisher-$PUBLISHER_NB
    dcos marathon app remove /benchmark/$MODE-publisher-$PUBLISHER_NB
  done
done

cd ../automation_scripts
echo "Removing Flink cluster"
./remove-flink-cluster.sh

### EVALUATION
echo "all jobs finished: ${topicnames[@]} with begintimes ${begintimes[@]} and endtimes ${endtimes[@]}"
cd ../automation_scripts
./start-spark-cluster.sh
for topic in  "${topicnames[@]}"; do
    echo "starting output consumer for $topic"
    ./run-output-consumer.sh $FRAMEWORK $MODE "$topic"
done

echo "starting evaluators for topics: ${topicnames[@]}"
for i in  "${!topicnames[@]}"; do
    echo "starting evaluator for ${topicnames[$i]}"
    echo "starting evaluator for begintimes ${begintimes[$i]} and endtimes ${endtimes[$i]}"
    ./run-evaluator.sh $FRAMEWORK $MODE "${stages[$i]}" "${topicnames[$i]}" ${begintimes[$i]} ${endtimes[$i]} $AMT_WORKERS $WORKER_CPU $WORKER_MEM $SPARK_EXECUTOR_MEMORY
done

sleep 30
./remove-spark-cluster.sh

echo "BENCHMARK FINISHED"
( speaker-test -t sine -f 500 )& pid=$! ; sleep 0.1s ; kill -9 $pid
