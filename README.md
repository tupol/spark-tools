# Spark Tools #

[![Maven Central](https://img.shields.io/maven-central/v/org.tupol/spark-tools_2.11.svg)][maven-central] &nbsp;
[![GitHub](https://img.shields.io/github/license/tupol/spark-tools.svg)][license] &nbsp; 
[![Travis (.org)](https://img.shields.io/travis/tupol/spark-tools.svg)][travis.org] &nbsp; 
[![Codecov](https://img.shields.io/codecov/c/github/tupol/spark-tools.svg)][codecov] &nbsp;
[![Javadocs](https://www.javadoc.io/badge/org.tupol/spark-tools_2.11.svg)][javadocs] &nbsp;
[![Gitter](https://badges.gitter.im/spark-tools/community.svg)][gitter] &nbsp; 
[![Twitter](https://img.shields.io/twitter/url/https/_tupol.svg?color=%2317A2F2)][twitter] &nbsp; 

## Description ##
This project contains some basic runnable tools that can help with various tasks around a Spark based project.

The main tools available:
- [FormatConverter](docs/format-converter.md) Converts any acceptable file format into a different
  file format, providing also partitioning support.
- [SimpleSqlProcessor](docs/sql-processor.md) Applies a given SQL to the input files which are 
  being mapped into tables.
- [StreamingFormatConverter](docs/streaming-format-converter.md) Converts any acceptable data 
  stream format into a different data stream format, providing also partitioning support.
- [SimpleFileStreamingSqlProcessor](docs/file-streaming-sql-processor.md) Applies a given SQL to the input files streams which are being mapped into file output streams.

This project is also trying to create and encourage a friendly yet professional environment 
for developers to help each other, so please do no be shy and join through [gitter], [twitter], 
[issue reports](https://github.com/tupol/spark-tools/issues/new/choose) or pull requests.


## Prerequisites ##

* Java 6 or higher
* Scala 2.11 or 2.12
* Apache Spark 2.3.X or higher


## Getting Spark Tools ##

Spark Tools is published to [Maven Central][maven-central] and [Spark Packages][spark-packages]:

where the latest artifacts can be found.

- Group id / organization: `org.tupol`
- Artifact id / name: `spark-tools`
- Latest version is `0.4.0`

Usage with SBT, adding a dependency to the latest version of tools to your sbt build definition file:

```scala
libraryDependencies += "org.tupol" %% "spark-tools" % "0.4.0"
```

Include this package in your Spark Applications using `spark-shell` or `spark-submit`
```bash
$SPARK_HOME/bin/spark-shell --packages org.tupol:spark-tools_2.11:0.4.0
```


## What's new? ##

**0.4.1**

- Added `StreamingFormatConverter`
- Added `FileStreamingSqlProcessor`, `SimpleFileStreamingSqlProcessor`
- Bumped `spark-utils` dependency to `0.4.1`

For previous versions please consult the [release notes](RELEASE-NOTES.md).

## License ##

This code is open source software licensed under the [MIT License](LICENSE).



[scala]: https://scala-lang.org/
[spark]: https://spark.apache.org/
[maven-central]: https://mvnrepository.com/artifact/org.tupol/spark-tools
[spark-packages]: https://spark-packages.org/package/tupol/spark-tools
[license]: https://github.com/tupol/spark-tools/blob/master/LICENSE
[travis.org]: https://travis-ci.com/tupol/spark-tools 
[codecov]: https://codecov.io/gh/tupol/spark-tools
[javadocs]: https://www.javadoc.io/doc/org.tupol/spark-tools_2.11
[gitter]: https://gitter.im/spark-tools/community
[twitter]: https://twitter.com/_tupol
