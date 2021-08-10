#! /bin/bash
export AMT_WORKERS=${1:-5}
export WORKER_CPU=${2:-4}
export WORKER_MEM=$(($WORKER_CPU*5))
export WORKER_MEM_MB=$(($WORKER_MEM*1024))
export DISK_MB=20480

if [[ $WORKER_CPU == 1 ]]; then
   export SIZE="-smallest"
elif [[ $WORKER_CPU == 2 ]]; then
   export SIZE="-small"
elif [[ $WORKER_CPU == 6 ]]; then
   export SIZE="-large"
else
   export SIZE=""
fi
echo "Size of the Flink cluster will be $SIZE"

cd ../aws_marathon_files
echo "Starting Flink with $AMT_WORKERS workers and $WORKER_CPU cpus"
envsubst < flink-jobmanager-with-env.json > flink-jobmanager.json
dcos marathon app add flink-jobmanager.json
echo 'Waiting for Flink jobmanager to start.'
sleep 30
echo 'Starting Flink taskmanagers'
for TASKMANAGER_NB in $(seq 1 $AMT_WORKERS)
do
  export TASKMANAGER_NB=$TASKMANAGER_NB
  envsubst < flink-taskmanager-with-env.json > flink-taskmanager-without-env-${TASKMANAGER_NB}.json
  dcos marathon app add flink-taskmanager-without-env-${TASKMANAGER_NB}.json
done

echo "will sleep 1 minutes for Flink to start up"
sleep 60
