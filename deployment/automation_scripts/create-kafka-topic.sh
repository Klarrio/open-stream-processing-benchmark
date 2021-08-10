#! /bin/bash
export TOPICNAME=$1
export NUM_PARTITIONS=${2:-20} # uses the input argument and otherwise 20 partitions
export TIMESTAMP_TYPE=${3:-LogAppendTime}


# configure Kafka manager
# First find the node on which the Kafka manager is running
KAFKA_BROKER_1=$(dcos task kafka-brokers | awk '{ print $2 }' | grep 10 | head -n1 | awk '{print $1;}')
echo "KAFKA_BROKER_1=$KAFKA_BROKER_1"

export KAFKA_BROKER_1_DOCKER_ID=$(ssh -oStrictHostKeyChecking=no core@$KAFKA_BROKER_1 docker ps | grep kafka-broker | awk '{print $1}')
echo "KAFKA_BROKER_1_DOCKER_ID=$KAFKA_BROKER_1_DOCKER_ID"


export TOPIC_CREATE_CMD="(/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --create --topic $TOPICNAME --partitions $NUM_PARTITIONS --replication-factor 1 --config message.timestamp.type=$TIMESTAMP_TYPE --zookeeper zk-1.zk:2181,zk-2.zk:2181,zk-3.zk:2181,zk-4.zk:2181,zk-5.zk:2181/kafka)"
export METRICS_TOPIC_CREATE_CMD="(/opt/kafka_2.11-2.1.0/bin/kafka-topics.sh --create --topic metrics-$TOPICNAME --partitions $NUM_PARTITIONS --replication-factor 1 --zookeeper zk-1.zk:2181,zk-2.zk:2181,zk-3.zk:2181,zk-4.zk:2181,zk-5.zk:2181/kafka)"


ssh -oStrictHostKeyChecking=no core@$KAFKA_BROKER_1 docker exec -i $KAFKA_BROKER_1_DOCKER_ID 'bash -c "'"$TOPIC_CREATE_CMD"'"'
ssh -oStrictHostKeyChecking=no core@$KAFKA_BROKER_1 docker exec -i $KAFKA_BROKER_1_DOCKER_ID 'bash -c "'"$METRICS_TOPIC_CREATE_CMD"'"'
