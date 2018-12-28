package org.tupol.spark.processors

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.io.parsers.JsonParserConfiguration
import org.tupol.spark.io.{ FileDataFrameLoaderConfig, FileDataFrameSaverConfig, FormatType }

import scala.util.Failure

class SqlProcessorConfigSpec extends FunSuite with Matchers {

  test("Load configuration with external sql local path even if sql.line is specified") {

    val config = ConfigFactory.parseString(
      """
        |  input: {
        |    # It contains a map of table names that need to be associated with the files
        |    tables {
        |     "table1": {
        |        path: "../../../src/test/resources/SqlProcessor/file1.json",
        |        format: "json"
        |     },
        |     "table2": {
        |        path: "../../../src/test/resources/SqlProcessor/file2.json",
        |        format: "json"
        |      }
        |    }
        |    variables {
        |       table_name: "table1"
        |       columns: "*"
        |    }
        |
        |    # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |    # The query must be written on a single line.
        |    # All quotes must be escaped.
        |    sql.line:"SELECT * FROM table1 where table1.id=\"1002\""
        |    sql.path: "src/test/resources/SqlProcessor/test.sql"
        |  }
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    partition.columns: ["id", "timestamp"]
        |  }
        |
      """.stripMargin)

    val expectedInputTablePaths = Map(
      "table1" -> FileDataFrameLoaderConfig("../../../src/test/resources/SqlProcessor/file1.json", JsonParserConfiguration()),
      "table2" -> FileDataFrameLoaderConfig("../../../src/test/resources/SqlProcessor/file2.json", JsonParserConfiguration()))

    val expectedSql =
      """-- Some comment
        |SELECT *
        |-- Some other comment
        |FROM table1
        |WHERE
        |-- Some filter comment
        |table1.id="1001"""".stripMargin

    val expectedVariables = Map("table_name" -> "table1", "columns" -> "*")

    val outputConfig = FileDataFrameSaverConfig("/tmp/tests/test.json", FormatType.Json, None, None, Seq[String]("id", "timestamp"))
    val expectedResult = SqlProcessorConfig(expectedInputTablePaths, expectedVariables, outputConfig, expectedSql)

    SqlProcessorConfig(config).get shouldBe expectedResult

  }

  test("Load configuration with external sql class path even if sql.line is specified") {

    val config = ConfigFactory.parseString(
      """
        |  input: {
        |    # It contains a map of table names that need to be associated with the files
        |    tables {
        |     "table1": {
        |        path: "../../../src/test/resources/SqlProcessor/file1.json",
        |        format: "json"
        |     },
        |     "table2": {
        |        path: "../../../src/test/resources/SqlProcessor/file2.json",
        |        format: "json"
        |      }
        |    }
        |    # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |    # The query must be written on a single line.
        |    # All quotes must be escaped.
        |    sql.line="SELECT * FROM table1 where table1.id=\"1002\""
        |    sql: { path: "/SqlProcessor/test.sql" }
        |  }
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    #partition.columns: ["id", "timestamp"]
        |  }
      """.stripMargin)

    val expectedInputTablePaths = Map(
      "table1" -> FileDataFrameLoaderConfig("../../../src/test/resources/SqlProcessor/file1.json", JsonParserConfiguration()),
      "table2" -> FileDataFrameLoaderConfig("../../../src/test/resources/SqlProcessor/file2.json", JsonParserConfiguration()))

    val expectedSql =
      """-- Some comment
        |SELECT *
        |-- Some other comment
        |FROM table1
        |WHERE
        |-- Some filter comment
        |table1.id="1001"""".stripMargin

    val outputConfig = FileDataFrameSaverConfig("/tmp/tests/test.json", FormatType.Json, None, None, Seq[String]())
    val expectedResult = SqlProcessorConfig(expectedInputTablePaths, outputConfig,
      expectedSql)

    SqlProcessorConfig(config).get shouldBe expectedResult

  }

  test("Load configuration from sql line") {

    val config = ConfigFactory.parseString(
      """
        |  input: {
        |    # It contains a map of table names that need to be associated with the files
        |    tables {
        |     "table1": {
        |        path: "../../../src/test/resources/SqlProcessor/file1.json",
        |        format: "json"
        |     },
        |     "table2": {
        |        path: "../../../src/test/resources/SqlProcessor/file2.json",
        |        format: "json"
        |      }
        |    }
        |    # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |    # The query must be written on a single line.
        |    # All quotes must be escaped.
        |    sql.line="SELECT * FROM table1 where table1.id=\"1002\""
        |    # sql: { path: "src/test/resources/SqlProcessor/test.sql" }
        |  }
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    #partition.columns: ["id", "timestamp"]
        |  }
        |
      """.stripMargin)

    val expectedInputTablePaths = Map(
      "table1" -> FileDataFrameLoaderConfig("../../../src/test/resources/SqlProcessor/file1.json", JsonParserConfiguration()),
      "table2" -> FileDataFrameLoaderConfig("../../../src/test/resources/SqlProcessor/file2.json", JsonParserConfiguration()))

    val expectedSql =
      """SELECT * FROM table1 where table1.id="1002"""".stripMargin

    val outputConfig = FileDataFrameSaverConfig("/tmp/tests/test.json", FormatType.Json, None, None, Seq[String]())
    val expectedResult = SqlProcessorConfig(expectedInputTablePaths, outputConfig, expectedSql)

    SqlProcessorConfig(config).get shouldBe expectedResult

  }

  test("Load configuration fails if neither sql.line nor sql.path are specified") {

    val config = ConfigFactory.parseString(
      """
        |  input: {
        |    # It contains a map of table names that need to be associated with the files
        |    tables {
        |     "table1": {
        |        path: "../../../src/test/resources/SqlProcessor/file1.json",
        |        format: "json"
        |     },
        |     "table2": {
        |        path: "../../../src/test/resources/SqlProcessor/file2.json",
        |        format: "json"
        |      }
        |    }
        |    # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |    # The query must be written on a single line.
        |    # All quotes must be escaped.
        |    # sql.line="SELECT * FROM table1 where table1.id=\"1002\""
        |    #sql: { path: "src/test/resources/SqlProcessor/test.sql" }
        |  }
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    partition.columns: ["id", "timestamp"]
        |  }
      """.stripMargin)

    SqlProcessorConfig(config) shouldBe a[Failure[_]]

  }

  test("replaceVariable returns the same text if no variables are defined") {

    val input = "SELECT {{columns}} FROM {{table.name}} WHERE {{condition_1}} AND a = '{{{condition-2}}}'"

    val result = SqlProcessorConfig.replaceVariables(input, Map())

    result shouldBe input
  }

  test("replaceVariable returns the same text where only the defined variables are replaced") {

    val input = "SELECT {{columns}} FROM {{table.name}} WHERE {{condition_1}} AND a = '{{{condition-2}}}'"

    val result = SqlProcessorConfig.replaceVariables(input, Map("columns" -> "a, b", "table.name" -> "some_table"))

    result shouldBe "SELECT a, b FROM some_table WHERE {{condition_1}} AND a = '{{{condition-2}}}'"
  }

  test("replaceVariable returns the text with all the variables replaced") {

    val input = "SELECT {{columns}} FROM {{table.name}} WHERE {{condition_1}} AND a = '{{{condition-2}}}'"

    val result = SqlProcessorConfig.replaceVariables(
      input,
      Map("columns" -> "a, b", "table.name" -> "some_table", "condition_1" -> "b='x'", "condition-2" -> "why"))

    result shouldBe "SELECT a, b FROM some_table WHERE b='x' AND a = '{why}'"
  }

}
