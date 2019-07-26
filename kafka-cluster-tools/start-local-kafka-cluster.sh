#! /bin/bash
export KAFKA_HOME=~/opt/kafka_2.11-2.1.0

sudo service zookeeper start

cd $KAFKA_HOME
./bin/kafka-server-stop.sh
./bin/kafka-server-start.sh config/server.properties
./bin/kafka-topics.sh --create --topic ndwflow --zookeeper localhost:2181 --partitions 20 --replication-factor 1
./bin/kafka-topics.sh --create --topic ndwspeed --zookeeper localhost:2181 --partitions 20 --replication-factor 1
./bin/kafka-topics.sh --create --topic metrics --zookeeper localhost:2181 --partitions 20 --replication-factor 1

