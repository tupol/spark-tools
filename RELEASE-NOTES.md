# Release Notes


## 0.4.0

- Added the `StreamingConfiguration` marker trait
- Added `GenericStreamDataSource`, `FileStreamDataSource` and `KafkaStreamDataSource`
- Added `GenericStreamDataSink`, `FileStreamDataSink` and `KafkaStreamDataSink`
- Added `FormatAwareStreamingSourceConfiguration` and `FormatAwareStreamingSinkConfiguration`
- Extracted `TypesafeConfigBuilder`
- API Changes: Added a new type parameter to the `DataSink` that describes the type of the output
- Improved unit test coverage


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
