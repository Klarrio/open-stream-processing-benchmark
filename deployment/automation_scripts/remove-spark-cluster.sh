#! /bin/bash
export SPARK_SERVICES=$(dcos marathon app list | grep spark- | awk '{ print $1 }')

for SERVICE in $SPARK_SERVICES
do
  echo "dcos marathon app stop $SERVICE"
  echo "dcos marathon app remove $SERVICE"
  dcos marathon app stop $SERVICE
  dcos marathon app remove $SERVICE
done
