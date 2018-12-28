# SimpleSqlProcessor


## Description

This application applies a sql query to the given input files producing an output file.
The input files need also a table name to run the SQL query against.

The SQL passed on can be parameterized. The variables for substitution are defined in the `input.variables` section
and they need to be specified inside the SQL wrapped by `{{` and `}}`.

## Application Parameters

- `SimpleSqlProcessor.input.tables`: contains a map of table names that need to be associated with the files;
  see the examples for more details on how to use and configure
- `SimpleSqlProcessor.input.variables`: an optional map of variables that should be substituted in the given SQL
- `SimpleSqlProcessor.input.sql.path`: the path to a file containing the SQL that needs to be ran;
        the processor will try loading the file from the local path, from a uri, from a url and finally from the local classpath;
        the local classpath is relative to the root of the jar and initial slash to the path is not required and should not be used;
        this has precedence over the `sql.line`.
- `SimpleSqlProcessor.input.sql.line`: the query that will be applied on the input tables;
        the result will be saved into the `SqlProcessor.output.path` file;
        ***the query must be written on a single line; all quotes must be escaped;***
        if both `sql.path` and `sql.line` are specified, the `sql.path` has precedence
- `SimpleSqlProcessor.output.path`: the path where the result will be saved
- `SimpleSqlProcessor.output.format`: the format of the output file; acceptable values are `json`, `avro`, `orc` and `parquet`
- `SimpleSqlProcessor.output.mode`: the save mode can be `overwrite`, `append`, `ignore` and `error`;
        more details available [here](https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/DataFrameWriter.html#mode-java.lang.String-)
- `SimpleSqlProcessor.output.partition.columns`: a sequence of columns that should be used for partitioning data on disk;
        they should exist in the result of the given sql; if empty no partitioning will be performed.
- `SimpleSqlProcessor.output.partition.files`: the number of partition files that will end up in each partition folder

The `SimpleSqlProcessor` uses the [`FileDataFrameLoader`](https://github.com/tupol/spark-utils/blob/master/docs/file-data-frame-loader.md)
and [`FileDataFrameSaver`](https://github.com/tupol/spark-utils/blob/master/docs/file-data-frame-saver.md) frameworks.
For more details please check the [`spark-utils`](https://github.com/tupol/spark-utils) project.

Each configured table is defined by the table name and the input file configuration, as defined by the
[`FileDataFrameLoader`](https://github.com/tupol/spark-utils/blob/master/docs/file-data-frame-loader.md).

## Usage Examples

In the [`examples`](examples/sql-processor) folder there are multiple examples on how the SQL processor can be used in
real life applications.

The [`sql-processor.sh`](examples/sql-processor/sql-processor.sh) is an example on how the actual application can be used.
Taking out the preparation part, it comes down to the following lines, which can be used in different setups, including
the `cluster-mode`.

```
spark-submit  -v  \
...
--class org.tupol.spark.processors.SimpleSqlProcessor \
--name SimpleSqlProcessor \
--files $APPLICATION_CONF \
--jars "$JARS" \
$SPARK_TOOLS_JAR
```

Notice the following variables:
- `$APPLICATION_CONF` needs to point to a file actually called `application.conf`, somewhere int the local file system.
- `$JARS` needs to contains the comma separated list of dependency jars; as of now this list contains the following jars:
`config-1.3.0.jar`, `scalaz-core_2.11-7.2.26.jar`, `scala-utils_2.11-0.2.0-SNAPSHOT.jar`, `spark-utils_2.11-0.2.0-SNAPSHOT.jar`
- `$SPARK_TOOLS_JAR` needs to contain the local path to the actual `spark-tools` jar; as of not this is `spark-tools_2.11-0.1.0-SNAPSHOT.jar`

Configuration Examples Description

- [`sample-application-1.conf`](examples/sql-processor/sample-application-1.conf)
Single input file, using an inline SQL and variables
- [`sample-application-2.conf`](examples/sql-processor/sample-application-2.conf)
Single input file using a more complex input parsing setup, with specified input schema
- [`sample-application-3.conf`](examples/sql-processor/sample-application-3.conf)
Two input files, using a more complex input parsing setup and a join query
- [`sample-application-4.conf`](examples/sql-processor/sample-application-4.conf)
Two input files, using an external query
