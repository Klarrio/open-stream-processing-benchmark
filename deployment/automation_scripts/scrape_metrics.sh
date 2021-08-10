#! /bin/bash
export DAY=4
export DAY1=$($DAY+1)
echo $DAY
echo $DAY1
export BEGINTIME=2020-06-0${DAY}T01:00:00Z
export ENDTIME=2020-06-0${DAY1}T01:00:00Z
export BEGINDATE=2020-06-04
export ENDDATE=2020-06-05

#export the network metrics of the run
AWS_INSTANCES_WITH_ID=(i-07f1a3e6ce1f0ff21 i-0701daf9df9cbebc4 i-039066e4556bd23c6 i-0e919e070ab450d2d i-0a6205b0d14716f5c i-01154a7ed1f20809d i-00cb3cc924dbe239d i-0fa6a2d39d1d3c828 i-0ddfcf7e382ab6559 i-049037d7ecf57c959 i-0a29d4d232dc99e61 i-0c2c97b3413f86291 i-0a0d39e01a10f63b1 i-0f4727f9a9061d3f6 i-01a862689d37c8aab i-0620481f2701b876b i-0dab73255c8b499b0 i-0f3ccedd7c7632733 i-055c929f428ec5284 i-0a3c9807c4e69eb1a i-0de7c538fe8fb2c20 i-0673a3e0864e2e486 i-07f354d53ba51c473)
METRICS_PATH="~/networkdata"
for instance_id in "${AWS_INSTANCES_WITH_ID[@]}"
  do
  echo $(aws cloudwatch get-metric-statistics --metric-name NetworkIn --region eu-west-1 --start-time $BEGINTIME --end-time $ENDTIME --period 60 --statistics Average --namespace AWS/EC2 --dimensions Name=InstanceId,Value=$instance_id | jq '.["Datapoints"]') >  "${BEGINDATE}_${ENDDATE}_${instance_id}_networkin_average.json"
  echo $(aws cloudwatch get-metric-statistics --metric-name NetworkOut --region eu-west-1 --start-time $BEGINTIME --end-time $ENDTIME --period 60 --statistics Average --namespace AWS/EC2 --dimensions Name=InstanceId,Value=$instance_id | jq '.["Datapoints"]') > "${BEGINDATE}_${ENDDATE}_${instance_id}_networkout_average.json"
  done
