#! /bin/bash
export TOPIC_NAME=${1:-metrics}

export KAFKA_ADVERTISED_HOST_NAME=$(hostname -I | head -n1 | awk '{print $1;}')

docker run --rm -it \
	wurstmeister/kafka:2.12-2.1.1 ./opt/kafka_2.12-2.1.1/bin/kafka-console-consumer.sh \
  --bootstrap-server $KAFKA_ADVERTISED_HOST_NAME:9092 \
  --topic $TOPIC_NAME
