package org.tupol.spark.tools

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType.Json
import org.tupol.spark.io.sources.JsonSourceConfiguration
import org.tupol.spark.io.streaming.structured.{ FileStreamDataSinkConfiguration, FileStreamDataSourceConfiguration, GenericStreamDataSinkConfiguration }
import org.tupol.spark.sql.loadSchemaFromFile
import org.tupol.spark.testing.files.TestTempFilePath1

import scala.util.Failure

class FileStreamingSqlProcessorContextSpec extends FunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  val ReferenceSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json")

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
      """.stripMargin
    )

    val expectedInputTables = Map(
      "table1" -> FileStreamDataSourceConfiguration(
        "../../../src/test/resources/SqlProcessor/file1.json", JsonSourceConfiguration()
      ),
      "table2" -> FileStreamDataSourceConfiguration(
        "../../../src/test/resources/SqlProcessor/file2.json", JsonSourceConfiguration()
      )
    )

    val expectedSql =
      """-- Some comment
        |SELECT *
        |-- Some other comment
        |FROM table1
        |WHERE
        |-- Some filter comment
        |table1.id="1001"""".stripMargin

    val expectedVariables = Map("table_name" -> "table1", "columns" -> "*")

    val expectedSink = FileStreamDataSinkConfiguration(
      "/tmp/tests/test.json",
      GenericStreamDataSinkConfiguration(format = Json, partitionColumns = Seq("id", "timestamp"))
    )
    val expected = FileStreamingSqlProcessorContext(expectedInputTables, expectedVariables, expectedSink, expectedSql)

    FileStreamingSqlProcessorContext(config).get shouldBe expected

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
        |    variables {
        |       table_name: "table1"
        |       columns: "*"
        |    }
        |
        |    # The query that will be applied on the input tables; the results will be saved into the SqlProcessor.output.path file
        |    # The query must be written on a single line.
        |    # All quotes must be escaped.
        |    # sql.line:"SELECT * FROM table1 where table1.id=\"1002\""
        |    # sql.path: "src/test/resources/SqlProcessor/test.sql"
        |  }
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

    FileStreamingSqlProcessorContext(config) shouldBe a[Failure[_]]

  }

}
