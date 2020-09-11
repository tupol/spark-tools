# SimpleFileStreamingSqlProcessor


## Description

The `SimpleFileStreamingSqlProcessor` is a basic concrete implementation of the 
`FileStreamingSqlProcessor` base class, which implements no custom user defined functions.


## Usage Examples

In the [`examples`](examples/file-streaming-sql-processor) folder there are multiple examples on how the SQL processor can be used in
real life applications.

The [`file-streaming-sql-processor.sh`](examples/file-streaming-sql-processor/file-streaming-sql-processor.sh) is an example on how the actual application can be used.
Taking out the preparation part, it comes down to the following lines, which can be used in different setups, including
the `cluster-mode`.

```
spark-submit  -v  \
...
--class org.tupol.spark.tools.SimpleFileStreamingSqlProcessor \
--name SimpleFileStreamingSqlProcessor \
--files $APPLICATION_CONF \
--jars "$JARS" \
$SPARK_TOOLS_JAR
```

Notice the following variables:
- `$APPLICATION_CONF` needs to point to a file actually called `application.conf`, somewhere int the local file system.
- `$JARS` needs to contains the comma separated list of dependency jars; as of now this list contains the following jars:
  - Scala 2.11: `config-1.3.0.jar`, `scalaz-core_2.11-7.2.26.jar`, `scala-utils_2.11-0.2.0.jar`, `spark-utils_2.11-0.4.2.jar`
  - Scala 2.12:  `config-1.3.0.jar`, `scalaz-core_2.12-7.2.26.jar`, `scala-utils_2.12-0.2.0.jar`, `spark-utils_2.12-0.4.2.jar`
- `$SPARK_TOOLS_JAR` needs to contain the local path to the actual `spark-tools` jar:
  - Scala 2.11: `spark-tools_2.11-0.4.0.jar`
  - Scala 2.12: `spark-tools_2.12-0.4.1-SNAPSHOT.jar`

**Configuration Examples Descriptions**

- [`sample-application-1.conf`](examples/file-streaming-sql-processor/sample-application-1.conf)
Single input file, using an inline SQL and variables
- [`sample-application-2.conf`](examples/file-streaming-sql-processor/sample-application-2.conf)
Single input file using a more complex input parsing setup, with specified input schema
- [`sample-application-3.conf`](examples/file-streaming-sql-processor/sample-application-3.conf)
Two input files, using a more complex input parsing setup and a join query
- [`sample-application-4.conf`](examples/file-streaming-sql-processor/sample-application-4.conf)
Two input files, using an external query, an external schema and variable substitution

Check the [README](examples/file-streaming-sql-processor/README.md) for more details on how to run
these examples.


## Application Parameters

See [`SqlProcessor`](file-streaming-sql-processor.md#configuration-parameters) for a detailed parameters description.

All application parameters need to be prefixed by `SimpleSqlProcessor`.
For example:
- `SimpleSqlProcessor.input.tables`
- `SimpleSqlProcessor.input...`
- `SimpleSqlProcessor.output...`

The `SimpleSqlProcessor` uses the [`spark-utils`](https://github.com/tupol/spark-utils/) defined IO framework.
For more details about defining the data sources please check the
[`DataDource`](https://github.com/tupol/spark-utils/blob/master/docs/data-source.md) documentation.

For more details about defining the data sinks please check the
[`DataSink`](https://github.com/tupol/spark-utils/blob/master/docs/data-sink.md) documentation.
