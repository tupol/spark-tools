# SqlProcessor


## Description

This application applies a sql query to the given input files or tables producing an output file.
The input files need also a table name to run the SQL query against.

The SQL passed on can be parameterized. The variables for substitution are defined in the `input.variables` section
and they need to be specified inside the SQL wrapped by `{{` and `}}`.


## Configuration Parameters

These application parameters need to be prefixed by the name of the class providing the concrete implementation.
Check [`SimpleSqlProcessor`](simple-sql-processr.md).

- `input.tables`: contains a map of table names that need to be associated with the input sources;
  see the provided [examples](examples/sql-processor) for more details on how to use and configure
- `input.variables`: an optional map of variables that should be substituted in the given SQL
- `input.sql.path`: the path to a file containing the SQL that needs to be ran;
        the processor will try loading the file from the local path, from a uri, from a url and finally from the local classpath;
        the local classpath is relative to the root of the jar and initial slash to the path is not required and should not be used;
        this has precedence over the `sql.line`.
- `input.sql.line`: the query that will be applied on the input tables;
        the result will be saved into the `SqlProcessor.output.path` file;
        ***the query must be written on a single line; all quotes must be escaped;***
        if both `sql.path` and `sql.line` are specified, the `sql.path` has precedence
- `output.path`: the path where the result will be saved
- `output.format`: the format of the output file; acceptable values are `json`, `avro`, `orc` and `parquet`
- `output.mode`: the save mode can be `overwrite`, `append`, `ignore` and `error`;
        more details available [here](https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/DataFrameWriter.html#mode-java.lang.String-)
- `output.partition.columns`: a sequence of columns that should be used for partitioning data on disk;
        they should exist in the result of the given sql; if empty no partitioning will be performed.
- `output.partition.files`: the number of partition files that will end up in each partition folder

The `SimpleSqlProcessor` uses the [`spark-utils`](https://github.com/tupol/spark-utils/) defined IO framework.
For more details about defining the data sources please check the
[`DataDource`](https://github.com/tupol/spark-utils/blob/master/docs/data-source.md) documentation.

For more details about defining the data sinks please check the
[`DataSink`](https://github.com/tupol/spark-utils/blob/master/docs/data-sink.md) documentation.


## Usage Examples

The main usage of the `SqlProcessor` base class is to implement it, while registering custom user defined functions
that are needed for the purpose of your project.
