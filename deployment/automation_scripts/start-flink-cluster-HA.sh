#! /bin/bash
export AMT_WORKERS=${1:-5}
export WORKER_CPU=${2:-4}
export WORKER_MEM=${3:-20}
export WORKER_MEM_MB=$(($WORKER_MEM*1024))
export DISK_MB=10240

export SIZE="-standby"

cd ../aws_marathon_files
echo 'Starting Flink'
envsubst < flink-jobmanager-with-env.json > flink-jobmanager.json
dcos marathon app add flink-jobmanager.json
sleep 20
dcos marathon app add flink-jobmanager-standby.json
echo 'Waiting for Flink jobmanager to start.'
sleep 30
echo 'Starting Flink taskmanagers'
for TASKMANAGER_NB in $(seq 1 $AMT_WORKERS)
do
  export TASKMANAGER_NB=$TASKMANAGER_NB
  envsubst < flink-taskmanager-with-env.json > flink-taskmanager-${TASKMANAGER_NB}.json
  dcos marathon app add flink-taskmanager-${TASKMANAGER_NB}.json
done

echo "will sleep 1 minutes for Flink to start up"
sleep 60
