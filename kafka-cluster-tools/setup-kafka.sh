#! /bin/bash

# Start up Zookeeper
docker run -d \
-p 2181:2181 \
jplock/zookeeper

# Start up Kafka
export IP_ADDR=$(hostname -I | head -n1 | awk '{print $1;}')
docker run -d \
	-p 9092:9092 \
	-e KAFKA_ADVERTISED_HOST_NAME=$IP_ADDR \
  	-e KAFKA_ADVERTISED_PORT="9092" \
	-e KAFKA_ZOOKEEPER_CONNECT=${IP_ADDR}:2181 \
	-e KAFKA_CREATE_TOPICS="ndwspeed:1:1,ndwflow:1:1,metrics:1:1,aggregation-data-topic:1:1,speed-through-topic:1:1,flow-through-topic:1:1" \
	wurstmeister/kafka:2.12-2.1.1

docker run -d \
	-p 82:80 \
	-p 2003:2003 \
	-p 3000:3000 \
	-p 7002:7002 \
	-v /var/lib/gmonitor/graphite:/var/lib/graphite/storage/whisper \
	-v /var/lib/gmonitor/grafana/data:/usr/share/grafana/data \
	kamon/grafana_graphite
