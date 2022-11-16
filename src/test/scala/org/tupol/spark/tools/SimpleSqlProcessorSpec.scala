package org.tupol.spark.tools

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.AnalysisException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.sources.JsonSourceConfiguration
import org.tupol.spark.io.{DataSinkException, DataSourceException, FileSinkConfiguration, FileSourceConfiguration, FormatType}
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.{TestTempFilePath1, TestTempFilePath2}
import org.tupol.spark.{SharedSparkSession, io}

import java.io.File

class SimpleSqlProcessorSpec extends AnyFunSuite with Matchers with SharedSparkSession
  with TestTempFilePath1 with TestTempFilePath2 {

  test("Select * from a single file") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration())
    )
    val sql = Sql.fromLine("SELECT * FROM table1").get

    val outputConfig = io.FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.compareWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select with variable substitution from a single file") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration())
    )
    val variables = Map("columns" -> "*", "table-name" -> "table1")
    val sql = Sql.fromLine("SELECT {{columns}} FROM {{table-name}}", variables).get

    val outputConfig = io.FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.compareWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select * from a single file with output partitions") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration())
    )
    val sql = Sql.fromLine("SELECT * FROM table1").get

    val outputConfig = io.FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]("id"))
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.compareWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select table1.* from two joined files") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val filePath2 = new File("src/test/resources/SqlProcessor/file2.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()),
      "table2" -> FileSourceConfiguration(filePath2, JsonSourceConfiguration())
    )
    val sql = Sql.fromLine("SELECT table1.* FROM table1 INNER JOIN table2 on table1.id == table2.id").get

    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)

    val result = SimpleSqlProcessor.run.get

    val expectedResult = spark.read.json(filePath1)
    result.schema shouldBe expectedResult.schema
    result.compareWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select table2.* from two joined files") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val filePath2 = new File("src/test/resources/SqlProcessor/file2.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration()),
      "table2" -> FileSourceConfiguration(filePath2, JsonSourceConfiguration())
    )
    val sql = Sql.fromLine("SELECT table2.* FROM table1 INNER JOIN table2 on table1.id == table2.id").get

    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)

    val result = SimpleSqlProcessor.run.get
    val expectedResult = spark.read.json(filePath2)
    result.schema shouldBe expectedResult.schema
    result.compareWith(expectedResult).areEqual(true) shouldBe true
  }

  test("Select with wrong table name yields an exception") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration())
    )
    val sql = Sql.fromLine("SELECT * FROM UNKNOWN_TABLE").get
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)

    an[AnalysisException] should be thrownBy SimpleSqlProcessor.run.get
  }

  test("SimpleSqlProcessor.buildConfig fails if the input configuration is incorrect") {
    val config = ConfigFactory.parseString("")
    an[Exception] should be thrownBy SimpleSqlProcessor.createContext(config).get
  }

  test("SimpleSqlProcessor.run fails if the input files can not be found") {
    val filePath1 = new File("/doodely/doo/floppity.flop").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration())
    )
    val sql = Sql.fromLine("SELECT * FROM table1").get
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)
    a[DataSourceException] should be thrownBy SimpleSqlProcessor.run.get
  }

  test("SimpleSqlProcessor.run fails if it is not possible to write to the output file") {
    val filePath1 = new File("src/test/resources/SqlProcessor/file1.json").getAbsolutePath
    val inputTables = Map(
      "table1" -> FileSourceConfiguration(filePath1, JsonSourceConfiguration())
    )
    val sql = Sql.fromLine("SELECT * FROM table1").get
    val outputConfig = FileSinkConfiguration(testPath1, FormatType.Json, None, None, Seq[String]())
    implicit val context = SqlProcessorContext(inputTables, outputConfig, sql)

    // The first time it works just fine
    noException shouldBe thrownBy(SimpleSqlProcessor.run)
    // The second time it fails to overwrite
    a[DataSinkException] should be thrownBy SimpleSqlProcessor.run.get
  }

}
