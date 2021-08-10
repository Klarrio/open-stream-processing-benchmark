#! /bin/bash

DCOS_DNS_ADDRESS=$(aws cloudformation describe-stacks --region eu-west-1 --stack-name=streaming-benchmark | jq '.Stacks[0].Outputs | .[] | select(.Description=="Master") | .OutputValue' |  awk '{print tolower($0)}')
export DCOS_DNS_ADDRESS="http://${DCOS_DNS_ADDRESS//\"}"
dcos config set core.dcos_url $DCOS_DNS_ADDRESS
dcos auth login
