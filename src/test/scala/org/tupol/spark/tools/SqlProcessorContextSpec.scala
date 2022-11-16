package org.tupol.spark.tools

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.io.pureconf._
import org.tupol.spark.io.pureconf.readers._
import pureconfig.generic.auto._
import org.tupol.spark.io.sources.JsonSourceConfiguration
import org.tupol.spark.io.{FileSinkConfiguration, FileSourceConfiguration, FormatType}

import scala.util.Failure

class SqlProcessorContextSpec extends AnyFunSuite with Matchers {

  test("Load configuration with external sql local path") {

    val config = ConfigFactory.parseString(
      """
        |  # It contains a map of table names that need to be associated with the files
        |  inputTables: {
        |    "table1": {
        |       path: "../../../src/test/resources/SqlProcessor/file1.json",
        |       format: "json"
        |    },
        |    "table2": {
        |       path: "../../../src/test/resources/SqlProcessor/file2.json",
        |       format: "json"
        |    }
        |  }
        |
        |  # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |  sql {
        |    path: "src/test/resources/SqlProcessor/test.sql"
        |    variables {
        |       table_name: "table1"
        |       columns: "*"
        |    }
        |  }
        |
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    partition.columns: ["id", "timestamp"]
        |  }
        |
      """.stripMargin
    )

    val expectedInputTablePaths = Map(
      "table1" -> FileSourceConfiguration("../../../src/test/resources/SqlProcessor/file1.json", JsonSourceConfiguration()),
      "table2" -> FileSourceConfiguration("../../../src/test/resources/SqlProcessor/file2.json", JsonSourceConfiguration())
    )

    val expectedVariables = Map("table_name" -> "table1", "columns" -> "*")

    val expectedSql = Sql.fromFile("src/test/resources/SqlProcessor/test.sql", expectedVariables).get

    val outputConfig = FileSinkConfiguration("/tmp/tests/test.json", FormatType.Json, None, None, Seq[String]("id", "timestamp"))
    val expectedResult = SqlProcessorContext(expectedInputTablePaths, outputConfig, expectedSql)

    config.extract[SqlProcessorContext].get shouldBe expectedResult

  }

  test("Fail to load configuration with external sql class path when sql.line is specified") {

    val config = ConfigFactory.parseString(
      """
        |  # It contains a map of table names that need to be associated with the files
        |  inputTables: {
        |    "table1": {
        |       path: "../../../src/test/resources/SqlProcessor/file1.json",
        |       format: "json"
        |    },
        |    "table2": {
        |       path: "../../../src/test/resources/SqlProcessor/file2.json",
        |       format: "json"
        |    }
        |  }
        |  # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |  sql {
        |    # The query must be written on a single line.
        |    # All quotes must be escaped.
        |    line: "SELECT * FROM table1 where table1.id=\"1002\""
        |    path: "/SqlProcessor/test.sql"
        |  }
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    #partition.columns: ["id", "timestamp"]
        |  }
      """.stripMargin
    )

    val expectedInputTablePaths = Map(
      "table1" -> FileSourceConfiguration("../../../src/test/resources/SqlProcessor/file1.json", JsonSourceConfiguration()),
      "table2" -> FileSourceConfiguration("../../../src/test/resources/SqlProcessor/file2.json", JsonSourceConfiguration())
    )

    config.extract[SqlProcessorContext] shouldBe a[Failure[_]]

  }

  test("Load configuration from sql line") {

    val config = ConfigFactory.parseString(
      """
        |  # It contains a map of table names that need to be associated with the files
        |  inputTables: {
        |    "table1": {
        |       path: "../../../src/test/resources/SqlProcessor/file1.json",
        |       format: "json"
        |    },
        |    "table2": {
        |       path: "../../../src/test/resources/SqlProcessor/file2.json",
        |       format: "json"
        |    }
        |  }
        |
        |  # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |  # The query must be written on a single line.
        |  # All quotes must be escaped.
        |  sql.line="SELECT * FROM table1 where table1.id=\"1002\""
        |  # sql: { path: "src/test/resources/SqlProcessor/test.sql" }
        |
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    #partition.columns: ["id", "timestamp"]
        |  }
        |
      """.stripMargin
    )

    val expectedInputTablePaths = Map(
      "table1" -> FileSourceConfiguration("../../../src/test/resources/SqlProcessor/file1.json", JsonSourceConfiguration()),
      "table2" -> FileSourceConfiguration("../../../src/test/resources/SqlProcessor/file2.json", JsonSourceConfiguration())
    )

    val expectedSql = Sql.fromLine("""SELECT * FROM table1 where table1.id="1002"""").get

    val outputConfig = FileSinkConfiguration("/tmp/tests/test.json", FormatType.Json, None, None, Seq[String]())
    val expectedResult = SqlProcessorContext(expectedInputTablePaths, outputConfig, expectedSql)

    config.extract[SqlProcessorContext].get shouldBe expectedResult

  }

  test("Load configuration fails if neither sql.line nor sql.path are specified") {

    val config = ConfigFactory.parseString(
      """
        |  # It contains a map of table names that need to be associated with the files
        |  inputTables: {
        |    "table1": {
        |       path: "../../../src/test/resources/SqlProcessor/file1.json",
        |       format: "json"
        |    },
        |    "table2": {
        |       path: "../../../src/test/resources/SqlProcessor/file2.json",
        |       format: "json"
        |    }
        |  }
        |
        |  # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |  # The query must be written on a single line.
        |  # All quotes must be escaped.
        |  # sql.line="SELECT * FROM table1 where table1.id=\"1002\""
        |  sql: {  }
        |
        |  output: {
        |    # The path where the results will be saved
        |    path: "/tmp/tests/test.json",
        |    # The format of the output file; acceptable values are "json", "avro", FormatType.Json and "parquet"
        |    format: "json",
        |    # The output partition columns
        |    partition.columns: ["id", "timestamp"]
        |  }
      """.stripMargin
    )

    config.extract[SqlProcessorContext] shouldBe a[Failure[_]]

  }

}
