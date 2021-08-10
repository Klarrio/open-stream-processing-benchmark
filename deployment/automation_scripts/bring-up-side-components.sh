#! /bin/bash
Help()
{
   # Display Help
   echo "This script spins up the benchmark services required to do a benchmark run."
   echo "In the beginning, it will prompt an external terminal to login to the DC/OS cluster running on AWS. Follow the instructions to continue the execution of the script."
   echo "It will wait 2 minutes for the login and then it will start spinning up services."
   echo ""
   echo "Parameters:"
   echo "-p    NUM_PARTITIONS: describing the number of partitions for the topics on Kafka. This also influences the number of brokers. (REQUIRED)"
   echo "-s    AMT_SLAVES: describing the number of slaves in the DC/OS cluster. Based on this it spins up enough ECR login and cAdvisor services. (REQUIRED)"
   echo "-k    KAFKA_BROKER_COUNT: describing the number of Kafka brokers. (REQUIRED)"
   echo "-h    Print this Help."
   echo ""
   echo "This script spins up the following services: "
   echo "1. Spark cluster: it will spin this up to make sure that there is enough place for a cluster to run afterwards. It actually reserves this place during the launch of other instances and then it is shut down again."
   echo "2. HDFS"
   echo "3. Kafka brokers"
   echo "4. Kafka Manager"
   echo "5. InfluxDB: to store cAdvisor data. The cadvisor database is also created here. "
   echo "6. cAdvisor: one instance per slave"
   echo "7. Removing spark cluster"
   echo "8. Create all necessary Kafka topics"
}
# Get the options
while getopts "hp:s:k:" option; do
   case $option in
      h) # display Help
          Help
          exit;;
      p)
          export NUM_PARTITIONS=${OPTARG}; echo "- NUM_PARTITIONS = $NUM_PARTITIONS"  ;;
      s)
          export AMT_SLAVES=${OPTARG}; echo "- AMT_SLAVES = $AMT_SLAVES"  ;;
      k)
          export KAFKA_BROKER_COUNT=${OPTARG}; echo "- KAFKA_BROKER_COUNT = $KAFKA_BROKER_COUNT"  ;;
      \?)
          echo "ERROR: Invalid option: -$OPTARG" >&2
          exit 1
        ;;
      :)
          echo "ERROR: Option -$OPTARG requires an argument." >&2
          exit 1
          ;;
      *)
          echo "ERROR: Option -$OPTARG requires an argument." >&2
          exit 1
          ;;
   esac
done
if [ $OPTIND -eq 1 ];
then
  echo "ERROR: No options were passed";
  Help ;
  exit 1
fi


export AWS_ACCESS_KEY=`cat AWS_ACCESS_KEY`
export AWS_SECRET_KEY=`cat AWS_SECRET_KEY`
if [ -z "$AWS_ACCESS_KEY" ] || [ -z "$AWS_SECRET_KEY" ] ; then
        echo 'ERROR: Missing AWS_ACCESS_KEY and/or AWS_SECRET_KEY. Fill it in in the AWS_ACCESS_KEY and AWS_SECRET_KEY files in the automation_scripts folder.' >&2
        exit 1
fi
if [ -z "$NUM_PARTITIONS" ] || [ -z "$AMT_SLAVES" ] || [ -z "$KAFKA_BROKER_COUNT" ]; then
        echo 'ERROR: Missing -p or -s or -k.' >&2
        exit 1
fi


DCOS_DNS_ADDRESS=$(aws cloudformation describe-stacks --region eu-west-1 --stack-name=streaming-benchmark | jq '.Stacks[0].Outputs | .[] | select(.Description=="Master") | .OutputValue' |  awk '{print tolower($0)}')
export DCOS_DNS_ADDRESS="http://${DCOS_DNS_ADDRESS//\"}"
sudo dcos cluster remove --all
sudo dcos cluster setup $DCOS_DNS_ADDRESS
sudo dcos cluster attach streaming-benchmark
dcos cluster remove --all
dcos cluster setup $DCOS_DNS_ADDRESS
dcos cluster attach streaming-benchmark
( speaker-test -t sine -f 500 )& pid=$! ; sleep 0.1s ; kill -9 $pid
##dcos auth login
gnome-terminal -e 'bash -c "sudo killall openvpn; ssh-add ~/.ssh/id_rsa_benchmark; sudo dcos auth login; sudo dcos package install tunnel-cli --cli --yes; sudo SSH_AUTH_SOCK=$SSH_AUTH_SOCK dcos tunnel --verbose vpn; bash"'
sleep 2m
echo "Installing Spark and Kafka CLI"
dcos package install spark --cli --yes
dcos package install kafka --cli --yes

## go to the folder with all the marathon definition files
cd ../aws_marathon_files



# start up spark to fill up the space
cd ../automation_scripts
./start-spark-cluster.sh
cd ../aws_marathon_files

# bring up HDFS
echo "Starting up HDFS"
dcos marathon app add aws-hdfs.json
sleep 8m

# bring up Kafka
echo "Starting up Kafka"
envsubst < aws-kafka-brokers-2.1-with-env.json > aws-kafka-brokers-2.1.json
dcos marathon app add aws-kafka-brokers-2.1.json
echo "will sleep 2 minutes to wait for full startup of Kafka brokers"
sleep 2m

# bring up Kafka Manager
echo "Starting up Kafka Manager"
dcos marathon app add aws-kafka-manager.json
echo "will sleep 60 seconds to wait for full startup of Kafka Manager"
sleep 60

# bring up InfluxDB
echo "Starting up InfluxDB"
dcos marathon app add aws-influx-db.json
echo "will sleep 60 seconds to wait for full startup of InfluxDB"
sleep 30
echo "creating cadvisor database"
INFLUXDB=$(dcos task influxdb | awk '{ print $2}' | grep 10)
curl -i -XPOST http://$INFLUXDB:8086/query --data-urlencode "q=CREATE DATABASE cadvisor"
sleep 30


# bring up cAdvisor
echo "Starting up cAdvisor"
envsubst < cadvisor-benchmark-with-env.json > cadvisor-benchmark.json
dcos marathon app add cadvisor-benchmark.json

## start up spark to fill up the space
cd ../automation_scripts
./remove-spark-cluster.sh
sleep 30
./set-up-kafka-manager.sh $NUM_PARTITIONS
