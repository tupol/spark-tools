# Streaming Format Converter


## Description

The `StreamingFormatConverter` application reads any Spark acceptable stream (`kafka`, `socket` or 
file streams like `parquet`, `avro`, `orc`, `json` or `csv`), converts it into a `DataFrame`, 
optionally partitions it, and saves it in any Spark acceptable data stream format 
(`kafka`, `socket` or file streams like `parquet`, `avro`, `orc`, `json` or `csv`).

One of the best features is the easy configuration and the support for output partitioning, by 
specifying both the partitioning column and the number of partition files per partition folder.

Full support for the parsing of various input files is provided.
For more details check the [Input Formats](#input-formats) section.


## Usage Examples

In the [`examples`](examples/streaming-format-converter) folder there are multiple examples on 
how the format converter can be used in real life applications.

The [`convert-format.sh`](examples/streaming-format-converter/convert-format.sh) is an example on 
how the actual application can be used.
Taking out the preparation part, it comes down to the following lines, which can be used in
different setups, including the `cluster-mode`.

```
spark-submit  -v  \
...
--class org.tupol.spark.tools.StreamingFormatConverter \
--name StreamingFormatConverter \
--files $APPLICATION_CONF \
--jars "$JARS" \
$SPARK_TOOLS_JAR
```

Notice the following variables:
- `$APPLICATION_CONF` needs to point to a file actually called `application.conf`, somewhere in 
  the local file system.
- `$JARS` needs to contains the comma separated list of dependency jars; as of now this list
  contains the following jars:
  - Scala 2.11: `config-1.4.2.jar`, `scalaz-core_2.12-7.2.28.jar`, `scala-utils_2.12-0.2.0.jar`, `spark-utils_2.12-0.4.2.jar`
  - Scala 2.12: `config-1.4.2.jar`, `scalaz-core_2.12-7.2.26.jar`, `scala-utils_2.12-0.2.0.jar`, `spark-utils_2.12-0.4.2.jar`
- `$SPARK_TOOLS_JAR` needs to contain the local path to the actual `spark-tools` jar:
  - Scala 2.11: `spark-tools_2.12-0.4.1.jar`
  - Scala 2.12: `spark-tools_2.12-0.4.1.jar`

**Configuration Examples Description**

- [`sample-application-1.conf`](examples/streaming-format-converter/sample-application-1.conf)
Socket stream to Json file stream conversion
- [`sample-application-2.conf`](examples/streaming-format-converter/sample-application-2.conf)
Json file stream to CSV file stream conversion
- [`sample-application-3.conf`](examples/streaming-format-converter/sample-application-3.conf)
Json file stream to CSV file stream conversion with partitioning columns
- [`sample-application-4.conf`](examples/streaming-format-converter/sample-application-4.conf)
Json file stream to CSV file stream conversion with partitioning columns and custom delimiter
- [`sample-application-5.conf`](examples/streaming-format-converter/sample-application-5.conf)
Json file stream to CSV file stream conversion
- [`sample-application-6.conf`](examples/streaming-format-converter/sample-application-6.conf)
Kafka stream to Json file stream conversion

Check the [README](examples/streaming-format-converter/README.md) for more details on how to run
these examples.

## Format Converter Parameters

### Common Parameters

- `StreamingFormatConverter.input.format` **Required**

- `StreamingFormatConverter.input.format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are: 
    - `socket`
    - `kafka`
    - file sources: `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `StreamingFormatConverter.input.schema` *Optional*
  - this is an optional parameter that represents the json Apache Spark schema that should be   
    enforced on the input data
  - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
  - due to it's complex structure, this parameter can not be passed as a command line argument, 
    but it can only be passed through the `application.conf` file
  - the schema is applied on read only on file streams; for other streams the developer has to 
    find a way to apply it.    
  - `StreamingFormatConverter.input.schema.path` *Optional*
    - this is an optional parameter that represents local path or the class path to the json 
      Apache Spark schema that should be enforced on the input data
    - this schema can be easily obtained from a `DataFrame` by calling the `prettyJson` function
    - if this parameter is found the schema will be loaded from the given file, otherwise, 
      the `schema` parameter is tried
- `StreamingFormatConverter.output.format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are: 
    - `kafka`
    - file sources: `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `StreamingFormatConverter.output.trigger` *Optional*
   - `type`: possible values: "Continuous", "Once", "ProcessingTime" 
   - `interval`: mandatory for "Continuous", "ProcessingTime" 
- `StreamingFormatConverter.output.queryName` *Optional*
- `StreamingFormatConverter.output.partition.columns` *Optional*
- `StreamingFormatConverter.output.outputMode` *Optional*
- `StreamingFormatConverter.output.checkpointLocation` *Optional*

The `StreamingFormatConverter` uses the [`spark-utils`](https://github.com/tupol/spark-utils/) defined IO framework.
For more details about defining the data sources please check the
[`DataDource`](https://github.com/tupol/spark-utils/blob/master/docs/data-source.md) documentation.

For more details about defining the data sinks please check the
[`DataSink`](https://github.com/tupol/spark-utils/blob/master/docs/data-sink.md) documentation.


### Input Parameters

#### File Parameters

- `path` **Required**
-  For more details check the [File Data Source](file-data-source.md#configuration-parameters)
   
#### Socket Parameters

**Warning: Not for production use!**

- `options` **Required**
  - `host` **Required**
  - `port` **Required**
  - `includeTimestamp` *Optional* 
   
#### Kafka Parameters

- `options` **Required**
  - `kafkaBootstrapServers` **Required** 
  - `assign` | `subscribe` | `subscribePattern` **Required** * 
  - `startingOffsets` *Optional* 
  - `endingOffsets` *Optional* 
  - `failOnDataLoss` *Optional* 
  - `kafkaConsumer.pollTimeoutMs` *Optional* 
  - `fetchOffset.numRetries` *Optional* 
  - `fetchOffset.retryIntervalMs` *Optional* 
  - `maxOffsetsPerTrigger` *Optional* 


### Output Parameters

#### Common Parameters

- `format` **Required**
  - the type of the input file and the corresponding source / parser
  - possible values are:  `xml`, `csv`, `json`, `parquet`, `avro`, `orc` and `text`
- `trigger` *Optional*
   - `type`: possible values: "Continuous", "Once", "ProcessingTime" 
   - `interval`: mandatory for "Continuous", "ProcessingTime" 
- `partition.columns` *Optional*
- `outputMode` *Optional*
- `checkpointLocation` *Optional*
  
#### File Parameters

- `path` **Required**
-  For more details check the [File Data Sink](file-data-sink.md#configuration-parameters)
  
#### Kafka Parameters

- `kafkaBootstrapServers` **Required** 
- `topic` **Required** 


## References

- For the CSV and JSON reader API see more details [here](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html).
- For the XML converter API see more details [here](https://github.com/databricks/spark-xml).
- For the Avro converter API see more details [here](https://github.com/databricks/spark-avro).
- [Structured Streaming Programming Guide - Input Sources](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources)
- [Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
