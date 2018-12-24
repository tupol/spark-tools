package org.tupol.spark.processors

import java.io.File

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.sources.JsonSourceConfiguration
import org.tupol.spark.{ SharedSparkSession, io }
import org.tupol.spark.io.{ FileSourceConfiguration, FileSinkConfiguration, FormatType }
import org.tupol.spark.testing.files.{ TestTempFilePath1, TestTempFilePath2 }
import org.tupol.spark.testing._

import scala.util.{ Failure, Success }

class SimpleSqlProcessorSpec extends FunSuite with Matchers with SharedSparkSession
  with TestTempFilePath1 with TestTempFilePath2 {

  test("Select * from a single file") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()))
    val sql = "SELECT * FROM table1"

    val outputConfig = io.FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val configuration = SqlProcessorConfig(inputTables, Map(), outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.comapreWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select with variable substitution from a single file") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()))
    val sql = "SELECT {{columns}} FROM {{table-name}}"
    val variables = Map("columns" -> "*", "table-name" -> "table1")
    val outputConfig = io.FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val configuration = SqlProcessorConfig(inputTables, variables, outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.comapreWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select * from a single file with output partitions") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()))
    val sql = "SELECT * FROM table1"

    val outputConfig = io.FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]("id"))
    implicit val configuration = SqlProcessorConfig(inputTables, Map(), outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.comapreWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select table1.* from two joined files") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val filePath2 = new File("src/test/resources/SqlProcessor/file2.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()),
      "table2" -> FileSourceConfiguration(filePath2, JsonSourceConfiguration()))
    val sql = "SELECT table1.* FROM table1 INNER JOIN table2 on table1.id == table2.id"

    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val configuration = SqlProcessorConfig(inputTables, Map(), outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.comapreWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select table2.* from two joined files") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val filePath2 = new File("src/test/resources/SqlProcessor/file2.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()),
      "table2" -> FileSourceConfiguration(filePath2, JsonSourceConfiguration()))
    val sql = "SELECT table2.* FROM table1 INNER JOIN table2 on table1.id == table2.id"

    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val configuration = SqlProcessorConfig(inputTables, Map(), outputConfig, sql)

    val result = SimpleSqlProcessor.run.get
    val expectedResult = spark.read.json(filePath2)
    result.schema shouldBe expectedResult.schema
    result.comapreWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select with wrong table name yields an exception") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()))
    val sql = "SELECT * FROM UNKNOWN_TABLE"
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val configuration = SqlProcessorConfig(inputTables, Map(), outputConfig, sql)

    SimpleSqlProcessor.run shouldBe a[Failure[_]]
  }

  test("SimpleSqlProcessor.buildConfig fails if the input configuration is incorrect") {
    val config = ConfigFactory.parseString("")
    SimpleSqlProcessor.buildConfig(config) shouldBe a[Failure[_]]
  }

  test("SimpleSqlProcessor.run fails if the input files can not be found") {
    val filePath1 = new File("/path/that/does/not/exist/nor/it/should/exist/no_name.unknown_extension").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()))
    val sql = "SELECT * FROM table1"
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val configuration = SqlProcessorConfig(inputTables, Map(), outputConfig, sql)

    SimpleSqlProcessor.run shouldBe a[Failure[_]]
  }

  test("SimpleSqlProcessor.run fails if it is not possible to write to the output file") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()))
    val sql = "SELECT * FROM table1"
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val configuration = SqlProcessorConfig(inputTables, Map(), outputConfig, sql)

    // The first time it works just fine
    SimpleSqlProcessor.run shouldBe a[Success[_]]
    // THe second time it fails to overwrite
    SimpleSqlProcessor.run shouldBe a[Failure[_]]
  }

}
