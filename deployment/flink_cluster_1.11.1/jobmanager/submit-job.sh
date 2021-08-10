#! /usr/bin/env bash
export JAR_NAME=$1

curl $JAR_NAME > flink-assembly.jar
sleep 15

export jobid=$(flink run -d flink-assembly.jar)
echo ${jobid##* } > job.txt
