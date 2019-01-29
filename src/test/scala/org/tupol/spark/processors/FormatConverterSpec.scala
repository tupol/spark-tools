package org.tupol.spark.processors

import org.apache.spark.sql.DataFrame
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.sources.{ AvroSourceConfiguration, ParquetSourceConfiguration }
import org.tupol.spark.io.{ FileSourceConfiguration, FileSinkConfiguration, FormatType }
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.TestTempFilePath1

class FormatConverterSpec extends FunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  test("FormatConverter basic run test from parquet to json") {

    val inputPath = "src/test/resources/sources/parquet/sample.parquet"
    val parserOptions = Map[String, String]()
    val parserConfig = ParquetSourceConfiguration(parserOptions, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val inputData = spark.source(inputConfig).read
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json)

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run

    val writtenData: DataFrame = spark.read.json(testPath1)

    returnedResult.comapreWith(inputData).areEqual(true) shouldBe true
    writtenData.comapreWith(inputData).areEqual(true) shouldBe true

  }

  test("FormatConverter basic run test from avro to json") {

    val inputPath = "src/test/resources/sources/avro/sample.avro"
    val parserOptions = Map[String, String]()
    val parserConfig = AvroSourceConfiguration(parserOptions, None)
    val inputConfig = FileSourceConfiguration(inputPath, parserConfig)
    val inputData = spark.source(inputConfig).read
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json)

    implicit val context = FormatConverterContext(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run

    val writtenData: DataFrame = spark.read.json(testPath1)

    returnedResult.comapreWith(inputData).areEqual(true) shouldBe true
    writtenData.comapreWith(inputData).areEqual(true) shouldBe true

  }

}
