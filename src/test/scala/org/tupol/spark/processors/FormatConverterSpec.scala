package org.tupol.spark.processors

import org.apache.spark.sql.DataFrame
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.implicits._
import org.tupol.spark.io.parsers.{ AvroConfiguration, ParquetConfiguration }
import org.tupol.spark.io.{ FileDataFrameLoaderConfig, FileDataFrameSaverConfig, FormatType }
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.TestTempFilePath1

class FormatConverterSpec extends FunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  test("FormatConverter basic run test from parquet to json") {

    val inputPath = "src/test/resources/parsers/parquet/sample.parquet"
    val parserOptions = Map[String, String]()
    val parserConfig = ParquetConfiguration(parserOptions, None)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val inputData = spark.loadData(inputConfig).get
    val outputConfig = FileDataFrameSaverConfig(testPath1, FormatType.Json)

    implicit val converterConfig = FormatConverterConfig(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run

    val writtenData: DataFrame = spark.read.json(testPath1)

    returnedResult.get.comapreWith(inputData).areEqual(true) shouldBe true
    writtenData.comapreWith(inputData).areEqual(true) shouldBe true

  }

  test("FormatConverter basic run test from avro to json") {

    val inputPath = "src/test/resources/parsers/avro/sample.avro"
    val parserOptions = Map[String, String]()
    val parserConfig = AvroConfiguration(parserOptions, None)
    val inputConfig = FileDataFrameLoaderConfig(inputPath, parserConfig)
    val inputData = spark.loadData(inputConfig).get
    val outputConfig = FileDataFrameSaverConfig(testPath1, FormatType.Json)

    implicit val converterConfig = FormatConverterConfig(inputConfig, outputConfig)

    val returnedResult = FormatConverter.run

    val writtenData: DataFrame = spark.read.json(testPath1)

    returnedResult.get.comapreWith(inputData).areEqual(true) shouldBe true
    writtenData.comapreWith(inputData).areEqual(true) shouldBe true

  }

}
