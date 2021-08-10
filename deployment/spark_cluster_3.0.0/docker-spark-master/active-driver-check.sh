#! /bin/bash
export SPARK_MASTER_IP=$1
cd /usr/local/spark
export APPS=$(curl --silent http://$SPARK_MASTER_IP:7777/json/applications)
export ACTIVE_DRIVER_ID=$(echo $APPS | jq -c '.activedrivers[0].id')
echo $ACTIVE_DRIVER_ID
