#! /bin/bash
export SPARK_MASTER_IP=$1
cd /usr/local/spark
export APPS=$(curl http://$SPARK_MASTER_IP:7777/json/applications)
export ACTIVE_DRIVER_ID=$(echo $APPS | jq -c '.activedrivers[0].id'| tr -d '"')

./bin/spark-class org.apache.spark.deploy.Client kill spark://spark-master.marathon.mesos:7077 $ACTIVE_DRIVER_ID
