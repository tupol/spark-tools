# Running the Examples

## Example 1

1. Start the net cat in a terminal
  ```console
  nc -l 9999
  ```
2. Start the app
  ```console
  ./convert-format.sh sample-application-1.conf
  ```
3. Type some lines in the `nc` terminal
4. Check the `tmp/out-example-1` folder for output


## Example 2

1. Create the input folder
  ```console
  mkdir -p tmp/in-example-2
  ```
2. Start the app
  ```console
  ./convert-format.sh sample-application-2.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file1.json tmp/in-example-2 5
  ```
4. Check the `tmp/out-example-2` folder for output


## Example 3

1. Create the input folder
  ```console
  mkdir -p tmp/in-example-3
  ```
2. Start the app
  ```console
  ./convert-format.sh sample-application-3.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file3.csv tmp/in-example-3 5
  ```
4. Check the `tmp/out-example-3` folder for output


## Example 4

1. Create the input folder
  ```console
  mkdir -p tmp/in-example-4
  ```
2. Start the app
  ```console
  ./convert-format.sh sample-application-4.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file3.csv tmp/in-example-4 5
  ```
4. Check the `tmp/out-example-4` folder for output


## Example 5

1. Create the input folder
  ```console
  mkdir -p tmp/in-example-5
  ```
2. Start the app
  ```console
  ./convert-format.sh sample-application-5.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file4.csv tmp/in-example-5 5
  ```
4. Check the `tmp/out-example-5` folder for output


## Example 6

1. Start Kafka producer

  Check the [Kafka notes](../KAFKA-NOTES.md)
  ```console
  kafka-topics.sh --zookeeper localhost:2181 --create --topic test-topic  --partitions 1 --replication-factor 1
  kafka-console-consumer.sh --zookeeper localhost:2181 --bootstrap-server localhost:9092 --topic test-topic
  kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic
  ```
2. Start the app
  ```console
  ./convert-format.sh sample-application-5.conf
  ```
3. Push some data files in the input stream folder
  ```console
  ../create-file-stream.sh resources/file4.csv tmp/in-example-5 5
  ```
4. Check the `tmp/out-example-5` folder for output


