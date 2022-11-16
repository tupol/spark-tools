package org.tupol.spark.tools

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.{ FileSinkConfiguration, FileSourceConfiguration, FormatType, JdbcSinkConfiguration }
import org.tupol.spark.io.sources.{ JdbcSourceConfiguration, TextSourceConfiguration }
import org.tupol.spark.sql.loadSchemaFromFile
import org.tupol.spark.testing.files.TestTempFilePath1

class FormatConverterContextSpec extends AnyFunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  val ReferenceSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json").get

  test("FormatConverterConfig basic creation jdbc to jdbc ") {
    val configStr =
      s"""
         |input.url="INPUT_URL"
         |input.table="SOURCE_TABLE"
         |input.user="USER_NAME"
         |input.password="USER_PASS"
         |input.driver="SOME_DRIVER"
         |input.options={
         |  opt1: "val1"
         |}
         |input.schema.path="/sources/avro/sample_schema.json"
         |output.format="jdbc"
         |output.url="OUTPUT_URL"
         |output.table="SOURCE_TABLE"
         |output.user="USER_NAME"
         |output.password="USER_PASS"
         |output.driver="SOME_DRIVER"
         |output.mode="SOME_MODE"
         |output.options={
         |  opt1: "val1"
         |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expectedSource = JdbcSourceConfiguration(
      url = "INPUT_URL",
      table = "SOURCE_TABLE",
      user = Some("USER_NAME"),
      password = Some("USER_PASS"),
      driver = Some("SOME_DRIVER"),
      options = Map("opt1" -> "val1"),
      schema = Some(ReferenceSchema)
    )
    val expectedSink = JdbcSinkConfiguration(
      url = "OUTPUT_URL",
      table = "SOURCE_TABLE",
      user = Some("USER_NAME"),
      password = Some("USER_PASS"),
      driver = Some("SOME_DRIVER"),
      mode = Some("SOME_MODE"),
      options = Map("opt1" -> "val1")
    )
    val expected = FormatConverterContext(expectedSource, expectedSink)

    val result = FormatConverterContext.create(config)
    result.get shouldBe expected
  }

  test("FormatConverterConfig basic creation file to jdbc ") {
    val configStr =
      s"""
         |input.path="INPUT_PATH"
         |input.format="text"
         |output.format="jdbc"
         |output.url="OUTPUT_URL"
         |output.table="SOURCE_TABLE"
         |output.user="USER_NAME"
         |output.password="USER_PASS"
         |output.driver="SOME_DRIVER"
         |output.mode="SOME_MODE"
         |output.options={
         |  opt1: "val1"
         |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expectedSource = FileSourceConfiguration(
      path = "INPUT_PATH",
      sourceConfiguration = TextSourceConfiguration()
    )
    val expectedSink = JdbcSinkConfiguration(
      url = "OUTPUT_URL",
      table = "SOURCE_TABLE",
      user = Some("USER_NAME"),
      password = Some("USER_PASS"),
      driver = Some("SOME_DRIVER"),
      mode = Some("SOME_MODE"),
      options = Map("opt1" -> "val1")
    )
    val expected = FormatConverterContext(expectedSource, expectedSink)

    val result = FormatConverterContext.create(config)
    result.get shouldBe expected
  }

  test("FormatConverterConfig basic creation jdbc to file ") {
    val configStr =
      s"""
         |input.url="INPUT_URL"
         |input.table="SOURCE_TABLE"
         |input.user="USER_NAME"
         |input.password="USER_PASS"
         |input.driver="SOME_DRIVER"
         |input.options={
         |  opt1: "val1"
         |}
         |input.schema: ${ReferenceSchema.prettyJson}
         |output.path="OUTPUT_PATH"
         |output.format="text"
         |output.mode="MODE"
         |output.partition.columns=["OUTPUT_PATH"]
         |output.partition.number=2
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expectedSource = JdbcSourceConfiguration(
      url = "INPUT_URL",
      table = "SOURCE_TABLE",
      user = Some("USER_NAME"),
      password = Some("USER_PASS"),
      driver = Some("SOME_DRIVER"),
      options = Map("opt1" -> "val1"),
      schema = Some(ReferenceSchema)
    )
    val expectedSink = FileSinkConfiguration(
      path = "OUTPUT_PATH",
      format = FormatType.Text,
      optionalSaveMode = Some("MODE"),
      partitionColumns = Seq("OUTPUT_PATH"),
      partitionFilesNumber = Some(2)
    )
    val expected = FormatConverterContext(expectedSource, expectedSink)

    val result = FormatConverterContext.create(config)
    result.get shouldBe expected
  }

}
