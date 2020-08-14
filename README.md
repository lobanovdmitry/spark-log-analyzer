# Spark Log Analyzer

Spark Log Analyzer is a Spark streaming application for logs analysis that collects events from Kafka, computes specific metrics based on incoming event and propagates stats back into Kafka.
The application consumes messages from a Kafka topic in format of JSON-array containing objects with fields: *timestamp*, *host*, *level* and *text*.
For example:
```json
[
    {"timestamp":"2018-04-02T21:03:17.294Z","host":"192.168.99.102","level":"ERROR","text":"descr"},
    {"timestamp":"2018-04-02T21:04:19.244Z","host":"192.168.99.102","level":"TRACE","text":"descr"},
    {"timestamp":"2018-04-02T21:06:17.694Z","host":"192.168.99.100","level":"DEBUG","text":"descr"}
]
```
The topology computes an averate rate of incoming log messages (number of messages per second) and a total number of events within a sliding window of 60 seconds for each log level: TRACE, DEBUG, INFO, WARN, ERROR. The application writes these metrics into a Kafka topic per host and log level.
In case if an average rate of ERROR logs for some host exceeds the threshold 1 message per second, then new event 'alert' is generated and is written into a Kafka topic 'alerts' as a JSON-object with fields: *timestamp*, *host* and *error_rate*.

### Building from sources
Prerequisites:
Make sure you have already installed [sbt](https://www.scala-sbt.org/download.html).
```
sbt clean package assembly
```
This will compile and build an 'uber'-jar which is ready to be deployed in Spark standalone cluster.

### Prepare environment
Prerequisites:
Make sure you have already installed both Docker Engine and Docker Compose.
```
mkdir /tmp/spark
cd ./docker
docker-compose up -d
```
This will start both Kafka and Spark standalone (master and worker nodes). Folder /tmp/spark will be used to share application jar file with master and worker of Spark. You can explore Kafka web-interface at http://localhost:3030, Spark-master at http://localhost:18080 and Spark-worker at http://localhost:18081.

### Submitting the application into Spark cluster
In order to start the application we need to 'submit' it. start-app.sh script contains all necessary statements to do that in docker environment. By default 'logs' Kafka topic used as an input source and 'summary' topic as an output sink for the application (can be changed in start-app.sh).
```
cp ./target/scala-2.11/spark-log-analyzer.jar /tmp/spark
cd ./docker
./start-app.sh
```
After the command is executed successfully you can navigate to Spark-master web-interface and track the application status.

### References:
1. http://spark.apache.org/streaming
2. https://kafka.apache.org
3. https://www.docker.com
4. https://docs.docker.com/compose/
5. https://github.com/Landoop/fast-data-dev
6. https://github.com/big-data-europe/docker-spark
