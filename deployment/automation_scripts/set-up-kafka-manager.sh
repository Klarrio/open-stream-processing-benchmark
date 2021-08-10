#! /bin/bash
export KAFKA_PARTITIONS=${1:-20}
# configure Kafka manager
# First find the node on which the Kafka manager is running
KAFKA_MANAGER_HOST=$(dcos task kafka-manager | awk '{ print $2 }' | grep 10)
# Add Kafka cluster to Kafka manager
curl "http://$KAFKA_MANAGER_HOST:9000/clusters" -H "Host: $KAFKA_MANAGER_HOST:9000" -H "Referer: http://$KAFKA_MANAGER_HOST:9000/addCluster" --data "name=benchmark&zkHosts=zk-1.zk%3A2181%2Czk-2.zk%3A2181%2Czk-3.zk%3A2181%2Czk-4.zk%3A2181%2Czk-5.zk%3A2181%2Fkafka&kafkaVersion=0.10.2.1&jmxEnabled=true&jmxUser=&jmxPass=&pollConsumers=true&tuning.brokerViewUpdatePeriodSeconds=30&tuning.clusterManagerThreadPoolSize=2&tuning.clusterManagerThreadPoolQueueSize=100&tuning.kafkaCommandThreadPoolSize=2&tuning.kafkaCommandThreadPoolQueueSize=100&tuning.logkafkaCommandThreadPoolSize=2&tuning.logkafkaCommandThreadPoolQueueSize=100&tuning.logkafkaUpdatePeriodSeconds=30&tuning.partitionOffsetCacheTimeoutSecs=5&tuning.brokerViewThreadPoolSize=4&tuning.brokerViewThreadPoolQueueSize=1000&tuning.offsetCacheThreadPoolSize=4&tuning.offsetCacheThreadPoolQueueSize=1000&tuning.kafkaAdminClientThreadPoolSize=4&tuning.kafkaAdminClientThreadPoolQueueSize=1000&securityProtocol=PLAINTEXT"

# create all kafka topics
./create-kafka-topic.sh ndwflow $KAFKA_PARTITIONS CreateTime
./create-kafka-topic.sh ndwspeed $KAFKA_PARTITIONS CreateTime
./create-kafka-topic.sh ndwspeedburst $KAFKA_PARTITIONS CreateTime
./create-kafka-topic.sh ndwflowburst $KAFKA_PARTITIONS CreateTime
./create-kafka-topic.sh ndwflowburst $KAFKA_PARTITIONS CreateTime #used by Kafka streams non incremental window after parsing tumbling window
./create-kafka-topic.sh aggregation-data-topic $KAFKA_PARTITIONS  CreateTime #used by Kafka streams custom tumbling window
./create-kafka-topic.sh relative-change-data-topic $KAFKA_PARTITIONS CreateTime #used by Kafka Streams custom sliding window
./create-kafka-topic.sh lane-aggregator-state-store $KAFKA_PARTITIONS CreateTime #used by Kafka streams custom tumbling window
./create-kafka-topic.sh relative-change-state-store $KAFKA_PARTITIONS CreateTime #used by Kafka Streams custom sliding window
./create-kafka-topic.sh flow-through-topic $KAFKA_PARTITIONS CreateTime #used by Kafka streams custom tumbling window
./create-kafka-topic.sh speed-through-topic $KAFKA_PARTITIONS CreateTime #used by Kafka Streams custom sliding window
