#! /bin/bash
Help()
{
   # Display Help
   echo "Structured Streaming job failure workload"
   echo
   echo "Parameters:"
   echo "-l    LAST_STAGE: describing the last stage (REQUIRED)"
   echo "-h    Print this Help."
   echo
}
# Get the options
while getopts "hl:" option; do
   case $option in
      h) # display Help
          Help
          exit;;
      l)
          export LAST_STAGE=${OPTARG} ;;
      \?)
          echo "ERROR: Invalid option: -$OPTARG" >&2
          exit 1
        ;;
      :)
          echo "ERROR: Option -$OPTARG requires an argument." >&2
          exit 1
          ;;
      *)
          echo "ERROR: Option -$OPTARG requires an argument." >&2
          exit 1
          ;;
   esac
done
if [ $OPTIND -eq 1 ];
then
  echo "ERROR: No options were passed";
  Help ;
  exit 1
fi


# hardcoded env vars
echo "Job Configuration:"
echo "- LAST_STAGE = $LAST_STAGE"
export FRAMEWORK="STRUCTUREDSTREAMING"; echo "- FRAMEWORK = $FRAMEWORK"
export MODE="faulty-event"; echo "- MODE = $MODE"
export JAR_PATH=`cat ../benchmark-jars-path`
export JAR_NAME=$JAR_PATH/structured-streaming-benchmark-assembly-3.0.jar; echo "- JAR_NAME = $JAR_NAME"
export FLOWTOPIC=ndwflow; echo "- FLOWTOPIC = $FLOWTOPIC"
export SPEEDTOPIC=ndwspeed; echo "- SPEEDTOPIC = $SPEEDTOPIC"
export PUBLISHER_COUNT=3
export KAFKA_AUTO_OFFSET_RESET_STRATEGY="latest"
export INPUT_DATA_PATH=`cat ../benchmark-input-data-path`

export AMT_WORKERS=5; echo "- AMT_WORKERS = $AMT_WORKERS"
export WORKER_CPU=4; echo "- WORKER_CPU = $WORKER_CPU"
export NUM_SQL_PARTITIONS=5

export AWS_ACCESS_KEY=`cat ../AWS_ACCESS_KEY`
export AWS_SECRET_KEY=`cat ../AWS_SECRET_KEY`
if [ -z "$AWS_ACCESS_KEY" ] || [ -z "$AWS_SECRET_KEY" ] ; then
        echo 'Missing AWS_ACCESS_KEY and/or AWS_SECRET_KEY. Fill it in in the AWS_ACCESS_KEY and AWS_SECRET_KEY files in the automation_scripts folder.' >&2
        exit 1
fi

# depending env vars
export WORKER_MEM=$(($WORKER_CPU*5))
export WORKER_HEAP_MEM=$(($WORKER_MEM*9/10-1))
export SPARK_EXECUTOR_MEMORY="${WORKER_HEAP_MEM}g"
export NUM_PARTITIONS=$(($WORKER_CPU*$AMT_WORKERS))
export SPARK_CORES_MAX=$(($AMT_WORKERS*$WORKER_CPU))
export CONC_GC_THREADS=$(($WORKER_CPU/2))
echo
echo

eval $(ssh-agent -s)
ssh-add ~/.ssh/id_rsa_benchmark

# start Spark cluster
cd ..
./start-spark-cluster.sh $AMT_WORKERS $WORKER_CPU
cd ../aws_marathon_files

SPARK_CONTAINER_NAME=($(dcos task spark | awk '{ print $1 }' | grep spark))
SPARK_CONTAINER_IP=($(dcos task spark | awk '{ print $2 }' | grep 10))

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
## get the Kafka brokers
BOOTSTRAP_SERVER_LIST=($(dcos task kafka-brokers | awk '{ print $2 }' | grep 10))
BROKER_LIST_STRING="${BOOTSTRAP_SERVER_LIST[*]}"
export KAFKA_BOOTSTRAP_SERVERS=$(echo "${BROKER_LIST_STRING//${IFS:0:1}/,}" | sed -E "s/([^,]+)/\1:10000/g")
echo "Kafka bootstrap servers configured as: $KAFKA_BOOTSTRAP_SERVERS"

# DCOS IP (for jmx exporter)
DCOS_DNS_ADDRESS=$(aws cloudformation describe-stacks --region eu-west-1 --stack-name=streaming-benchmark | jq '.Stacks[0].Outputs | .[] | select(.Description=="Master") | .OutputValue' |  awk '{print tolower($0)}')
export CLUSTER_URL=http://${DCOS_DNS_ADDRESS//\"}
echo $CLUSTER_URL

# DCOS access token (for jmx exporter)
export DCOS_ACCESS_TOKEN=$(dcos config show core.dcos_acs_token)
echo $DCOS_ACCESS_TOKEN


topicnames=()
begintimes=()
endtimes=()

volume_array=(45)
for DATA_VOLUME in "${volume_array[@]}"
do
  export DATA_VOLUME=$DATA_VOLUME
  cd ../aws_marathon_files

  export VOLUME_PER_PUBLISHER=$((($DATA_VOLUME+($PUBLISHER_COUNT-1))/$PUBLISHER_COUNT))
  echo "adding publishers"
  for PUBLISHER_NB in $(seq 1 $PUBLISHER_COUNT)
  do
    export PUBLISHER_NB=$PUBLISHER_NB
    envsubst < aws-publisher-with-env.json > aws-publisher-without-env-$PUBLISHER_NB.json
    dcos marathon app add aws-publisher-without-env-$PUBLISHER_NB.json
  done

  echo "starting buffer timeout $BUFFER_TIMEOUT and volume $DATA_VOLUME"
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
  echo "starting new benchmark run for stage $LAST_STAGE on topic $TOPICNAME: buffer timeout $BUFFER_TIMEOUT and volume $DATA_VOLUME"

  # Spark standalone cluster deployment
  envsubst < spark-submit-with-env.json > spark-submit.json
  dcos marathon app add spark-submit.json
  sleep 30

  # get the hosts of the jmx containers
  SPARK_CONTAINER_NAME=($(dcos task spark | awk '{ print $1 }' | grep spark))
  SPARK_CONTAINER_IP=($(dcos task spark | awk '{ print $2 }' | grep 10))
  export JMX_HOSTS=""
  # we also need to monitor spark submit
  export NUM_ENTRIES=$(($AMT_WORKERS+1))
  for i in $(seq 0 $NUM_ENTRIES)
  do
    JMX_HOSTS="${JMX_HOSTS}${SPARK_CONTAINER_NAME[$i]}:${SPARK_CONTAINER_IP[$i]},"
  done
  JMX_HOSTS=${JMX_HOSTS::-1} # remove the last comma
  echo $JMX_HOSTS
  echo "cadvisor hosts of spark containers"
  export CADVISOR_HOSTS=""
  for i in $(seq 0 $NUM_ENTRIES)
  do
    CADVISOR_HOSTS="${CADVISOR_HOSTS}${SPARK_CONTAINER_IP[$i]}:8888,"
  done
  CADVISOR_HOSTS=${CADVISOR_HOSTS::-1} # remove the last comma
  echo $CADVISOR_HOSTS

  # Start up the jmx metrics gathering
  echo "Start up JMX metrics exporter"
  envsubst < jmx-exporter-with-env.json > jmx-exporter-without-env.json
  dcos marathon app add jmx-exporter-without-env.json

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
	for k in {1..15}
	do
	    sleep 1m
	    echo "benchmark running for $k minutes"
	done


	##################  END OF RUN ######################
	# kill metrics-exporter
	echo "Killing jmx-exporter for stage  dcos marathon app stop flink-taskmanager-1
  sleep 10
  dcos marathon app start flink-taskmanager-1 $LAST_STAGE"
	dcos marathon app stop jmx-exporter
  dcos marathon app remove jmx-exporter

  # kill spark job
  #####dcos spark kill $SPARK_ID
  dcos marathon app stop spark-submit
  dcos marathon app remove spark-submit

  endtime=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "endtime of $TOPICNAME - $endtime"
  endtimes+=($endtime)

  # kill ndw-publisher
  echo "Killing ndw-publisher for stage $LAST_STAGE"
  for PUBLISHER_NB in $(seq 1 $PUBLISHER_COUNT)
  do
    export PUBLISHER_NB=$PUBLISHER_NB
    dcos marathon app stop /benchmark/$MODE-publisher-$PUBLISHER_NB
    dcos marathon app remove /benchmark/$MODE-publisher-$PUBLISHER_NB
  done

  sleep 1m
done

### EVALUATION
echo "all jobs finished: ${topicnames[@]} with begintimes ${begintimes[@]} and endtimes ${endtimes[@]}"
cd ../automation_scripts

for topic in  "${topicnames[@]}"; do
    echo "starting output consumer for $topic"
    ./run-output-consumer.sh $FRAMEWORK $MODE "$topic"
done

echo "starting evaluators for topics: ${topicnames[@]}"
for i in  "${!topicnames[@]}"; do
    echo "starting evaluator for ${topicnames[$i]}"
    echo "starting evaluator for begintimes ${begintimes[$i]} and endtimes ${endtimes[$i]}"
    ./run-evaluator.sh $FRAMEWORK $MODE $LAST_STAGE "${topicnames[$i]}" ${begintimes[$i]} ${endtimes[$i]} $AMT_WORKERS $WORKER_CPU $WORKER_MEM $SPARK_EXECUTOR_MEMORY
done


sleep 30
./remove-spark-cluster.sh

echo "BENCHMARK FINISHED"
( speaker-test -t sine -f 500 )& pid=$! ; sleep 0.1s ; kill -9 $pid
