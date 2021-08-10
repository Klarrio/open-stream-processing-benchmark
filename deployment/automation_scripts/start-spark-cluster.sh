#! /bin/bash
export AMT_WORKERS=${1:-5}
export WORKER_CPU=${2:-4}
export WORKER_MEM=$(($WORKER_CPU*5))
export WORKER_MEM_MB=$(($WORKER_MEM*1024))
export DISK_MB=20480

cd ../aws_marathon_files
echo 'Starting Spark'
dcos marathon app add spark-master.json
echo 'Waiting for Spark master to start.'
sleep 20
echo 'Starting Spark workers'

for WORKER_NB in $(seq 1 $AMT_WORKERS)
do
  export WORKER_NB=$WORKER_NB
  envsubst < spark-worker-with-env.json > spark-worker-without-env-$WORKER_NB.json
  dcos marathon app add spark-worker-without-env-$WORKER_NB.json
done
echo "will sleep 1 minutes for spark to start up"
sleep 60
