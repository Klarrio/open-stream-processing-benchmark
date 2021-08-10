#! /bin/bash
dcos marathon app remove jmx-exporter

export PUBLISHER_SERVICES=$(dcos marathon app list | grep /benchmark/ | awk '{ print $1 }')

for SERVICE in $PUBLISHER_SERVICES
do
  echo "dcos marathon app stop $SERVICE"
  echo "dcos marathon app remove $SERVICE"
  dcos marathon app stop $SERVICE
  dcos marathon app remove $SERVICE
done
