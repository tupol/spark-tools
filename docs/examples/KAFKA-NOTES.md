# Kafka Notes

## Setting Up Kafka 0.10.0.1

- https://archive.apache.org/dist/kafka/0.10.0.1/kafka_2.11-0.10.0.1.tgz


## Setting up Zookeeper

- https://archive.apache.org/dist/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz 

## Start Zookeeper

```
$ZOOKEEPER_HOME/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```

## Start Kafka

```
$KAFKA_HOME/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```
