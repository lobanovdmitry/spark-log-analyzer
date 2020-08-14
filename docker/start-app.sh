#!/bin/bash

SPARK_MASTER_ID=`docker ps --filter "ancestor=bde2020/spark-master:2.3.0-hadoop2.7" --format {{.ID}}`

if [[ -z $SPARK_MASTER_ID ]]
then
  echo "Unable to start application: no running spark master container."
  exit 1
fi

COMMAND_CLUSTER="spark/bin/spark-submit \
                             --deploy-mode cluster \
                             --master spark://spark-master:7077 \
                             --class ru.dlobanov.loganalyzer.Main \
                             /tmp/spark/spark-log-analyzer.jar \
                             kafka:9092 logs summary"

echo $COMMAND_CLUSTER

docker exec -it $SPARK_MASTER_ID $COMMAND_CLUSTER

echo "The application has been submitted, track the status via spark master web-interface http://localhost:18080"
