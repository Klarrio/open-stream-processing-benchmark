# OSPBench Metrics Exporter

This component exports JMX and cAdvisor metrics from containers and writes them to Kafka.
It collects metrics from the framework cluster containers, Kafka brokers and HDFS components.


##  Deployment
This component requires the following environment variables:

- `FRAMEWORK`: either FLINK/KAFKASTREAMS/SPARK/STRUCTUREDSTREAMIGNG
- `TOPICNAME`: name of the topic currently used for the benchmark run. This component will publish to the topic `metrics-$TOPICNAME`.
- `JMX_HOSTS`: the hosts from which metrics should be scraped, so the host of each framework cluster component.
- `CLUSTER_URL`: IP of the DC/OS cluster. We retrieve this in our scripts with:

      DCOS_DNS_ADDRESS=$(aws cloudformation describe-stacks --region eu-west-1 --stack-name=streaming-benchmark | jq '.Stacks[0].Outputs | .[] | select(.Description=="Master") | .OutputValue' |  awk '{print tolower($0)}')
      export CLUSTER_URL=http://${DCOS_DNS_ADDRESS//\"}
      echo $CLUSTER_UR

- `DCOS_ACCESS_TOKEN`: token to access DC/OS. We retrieve this in our scripts with:

      dcos config show core.dcos_acs_token

- `CADVISOR_HOSTS`: cadvisor hosts
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka brokers


You can run this component as a Docker container next to cAdvisor and a framework cluster.
