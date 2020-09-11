# Release Notes

##  0.4.1
- Added `StreamingFormatConverter`
- Added `FileStreamingSqlProcessor`, `SimpleFileStreamingSqlProcessor`
- Bumped `spark-utils` dependency to `0.4.2`
- The project compiles with both Scala `2.11.12` and `2.12.12`
- Updated Apache Spark to `2.4.6`
- Updated `delta.io` to `0.6.1`
- Updated the `spark-xml` library to `0.10.0`
- Removed the `com.databricks:spark-avro` dependency, as avro support is now built into Apache Spark
- Updated the `spark-utils` dependency to the latest available snapshot


## 0.4.0

- Added `StreamingFormatConverter`
- Added `FileStreamingSqlProcessor`, `SimpleFileStreamingSqlProcessor`
- Bumped `spark-utils` dependency to `0.4.1`

## 0.3.0

- Package `processors` was renamed to `tools`
- `SqlProcessor.registerSqlFunctions` takes now implicit parameters: spark session and 
  application context
- Added `StreamingFormatConverter`
- Added `FileStreamingSqlProcessor`, `SimpleFileStreamingSqlProcessor`

## 0.2.1

- Started using `spark-utils` `0.3.1` to benefit from variable substitution

## 0.2.0

- Started using `spark-utils` `0.3.0` and made the necessary API changes

## 0.1.0

- Added `FormatConverter`
- Added `SqlProcessor` base class
- Added `SimpleSqlProcessor` implementation
