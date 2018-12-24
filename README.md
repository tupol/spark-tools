# Spark Tools #


## Description ##
This project contains some basic runnable tools that can help with various tasks around a Spark based project.

The main tools available:
- [FormatConverter](docs/format-converter.md) Converts any acceptable file format into a different file format,
    providing also partitioning support.
- [SimpleSqlProcessor](docs/sql-processor.md) Applies a given SQL to the input files which are being mapped into tables.


## Prerequisites ##

* Java 6 or higher
* Scala 2.11,or 2.12


## Getting Spark Tools ##

Spark Tools is published to Sonatype OSS and Maven Central:

- Group id / organization: `org.tupol`
- Artifact id / name: `spark-tools`
- Latest version is `0.1.0-SNAPSHOT`

Usage with SBT, adding a dependency to the latest version of tools to your sbt build definition file:

```scala
libraryDependencies += "org.tupol" %% "spark-tools" % "0.1.0-SNAPSHOT"
```


## What's new? ##

### 0.1.0-SNAPSHOT ###
 - Added `FormatConverter`
 - Added `SqlProcessor` base class
 - Added `SimpleSqlProcessor` implementation


## License ##

This code is open source software licensed under the [MIT License](LICENSE).
