#! /bin/bash
export FLINK_SERVICES=$(dcos marathon app list | grep flink- | awk '{ print $1 }')

for SERVICE in $FLINK_SERVICES
do
  echo "dcos marathon app stop $SERVICE"
  echo "dcos marathon app remove $SERVICE"
  dcos marathon app stop $SERVICE
  dcos marathon app remove $SERVICE
done
