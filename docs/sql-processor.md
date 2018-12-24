# SimpleSqlProcessor


## Description

This application applies a sql query to the given input files producing an output file.
The input files need also a table name to run the SQL query against.

## Application Parameters

- `SqlProcessor.input.tables`: contains a map of table names that need to be associated with the files
- `SqlProcessor.input.variables`: an optional map of variables that should be substituted in the given SQL
- `SqlProcessor.output.path`: the path where the result will be saved
- `SqlProcessor.output.format`: the format of the output file; acceptable values are `json`, `avro`, `orc` and `parquet`
- `SqlProcessor.output.mode`: the save mode can be `overwrite`, `append`, `ignore` and `error`;
        more details available [here](https://spark.apache.org/docs/2.3.1/api/java/org/apache/spark/sql/DataFrameWriter.html#mode-java.lang.String-)
- `SqlProcessor.output.partition.columns`: a sequence of columns that should be used for partitioning data on disk;
        they should exist in the result of the given sql; if empty no partitioning will be performed.
- `SqlProcessor.output.partition.number`: the number of partition files that will end up in each partition folder
- `SqlProcessor.sql.path`: the path to a file containing the SQL that needs to be ran;
        the processor will try loading the file from the local path, from a uri, from a url and finally from the local classpath;
        the local classpath is relative to the root of the jar and initial slash to the path is not required and should not be used;
        this has precedence over the `sql.line`.
- `SqlProcessor.sql.line`: the query that will be applied on the input tables;
        the result will be saved into the `SqlProcessor.output.path` file;
        ***the query must be written on a single line; all quotes must be escaped;***
        if both specified, the `sql.path` has precedence
