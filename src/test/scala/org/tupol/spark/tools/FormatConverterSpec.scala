package org.tupol.spark.tools

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.sources.{ AvroSourceConfiguration, GenericSourceConfiguration, ParquetSourceConfiguration }
import org.tupol.spark.io.{ FileSinkConfiguration, FileSourceConfiguration, FormatType, GenericSinkConfiguration }
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.TestTempFilePath1

class FormatConverterSpec extends AnyFunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  test("FormatConverter basic run test from parquet to json") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val parserOptions = Map[String, String]()
    val parserConfig = ParquetSourceConfiguration(parserOptions, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val inputData = spark.source(inputConfig).read.get
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json)

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run.get

    val writtenData: DataFrame = spark.read.json(testPath1)

    returnedResult.compareWith(inputData).areEqual(true) shouldBe true
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

  test("FormatConverter basic run test from databricks avro to json") {

    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val parserOptions = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(parserOptions, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val inputData = spark.source(inputConfig).read.get
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json)

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run.get

    val writtenData: DataFrame = spark.read.json(testPath1)

    returnedResult.compareWith(inputData).areEqual(true) shouldBe true
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

  test("FormatConverter basic run test from spark avro to json") {

    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val options = Map[String, String]("path" -> inputPath)
    val inputConfig = GenericSourceConfiguration(FormatType.Custom("avro"), options)

    val inputData = spark.source(inputConfig).read.get
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json)

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run.get

    val writtenData: DataFrame = spark.read.json(testPath1)

    returnedResult.compareWith(inputData).areEqual(true) shouldBe true
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

  test("FormatConverter basic run test from parquet to databricks avro") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val parserOptions = Map[String, String]()
    val parserConfig = ParquetSourceConfiguration(parserOptions, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val inputData = spark.source(inputConfig).read.get
    val writerOptions = Map[String, String]("path" -> testPath1)
    val outputConfig = GenericSinkConfiguration(
      FormatType.Custom("avro"),
      None, Seq(), None, writerOptions
    )

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run.get

    val writtenData: DataFrame = spark.read.format("avro").load(testPath1)

    returnedResult.compareWith(inputData).areEqual(true) shouldBe true
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

  test("FormatConverter basic run test from parquet to spark avro") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val parserOptions = Map[String, String]()
    val parserConfig = ParquetSourceConfiguration(parserOptions, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val inputData = spark.source(inputConfig).read.get
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Avro)

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run.get

    val writtenData: DataFrame = spark.read.format("avro").load(testPath1)

    returnedResult.compareWith(inputData).areEqual(true) shouldBe true
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

  test("FormatConverter basic run test from parquet to delta") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val parserOptions = Map[String, String]()
    val parserConfig = ParquetSourceConfiguration(parserOptions, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val inputData = spark.source(inputConfig).read.get
    val writerOptions = Map[String, String]("path" -> testPath1)
    val outputConfig = GenericSinkConfiguration(
      FormatType.Custom("delta"),
      None, Seq(), None, writerOptions
    )

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run.get

    val writtenData: DataFrame = spark.read.format("delta").load(testPath1)

    returnedResult.compareWith(inputData).areEqual(true) shouldBe true
    writtenData.compareWith(inputData).areEqual(true) shouldBe true
  }

}
