#!/bin/bash

sudo snap remove --purge spark-client
sudo snap install spark-client
mkdir -p ~/.kube
sudo microk8s config | tee ~/.kube/config

spark-client.service-account-registry delete --username hello

spark-client.service-account-registry create --username hello \
    --conf spark.eventLog.dir=s3a://history-server/spark-events/ \
    --conf spark.history.fs.logDirectory=s3a://history-server/spark-events/

spark-client.service-account-registry get-config --username hello