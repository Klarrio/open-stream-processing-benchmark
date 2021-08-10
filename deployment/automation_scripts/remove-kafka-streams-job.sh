#! /bin/bash
export KAFKA_STREAMS_SERVICES=$(dcos marathon app list | grep kafka-streams/kafka-thread | awk '{ print $1 }')

for SERVICE in $KAFKA_STREAMS_SERVICES
do
  echo "dcos marathon app stop $SERVICE"
  echo "dcos marathon app remove $SERVICE"
  dcos marathon app stop $SERVICE
  dcos marathon app remove $SERVICE
done
