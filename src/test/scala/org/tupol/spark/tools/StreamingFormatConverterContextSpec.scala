package org.tupol.spark.tools

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType.{ Json, Kafka }
import org.tupol.spark.io.sources.TextSourceConfiguration
import org.tupol.spark.io.streaming.structured.{ FileStreamDataSinkConfiguration, FileStreamDataSourceConfiguration, GenericStreamDataSinkConfiguration, KafkaStreamDataSinkConfiguration, KafkaStreamDataSourceConfiguration, KafkaSubscription }
import org.tupol.spark.sql.loadSchemaFromFile
import org.tupol.spark.testing.files.TestTempFilePath1

class StreamingFormatConverterContextSpec extends FunSuite with Matchers with SharedSparkSession with TestTempFilePath1 {

  val ReferenceSchema = loadSchemaFromFile("src/test/resources/sources/avro/sample_schema.json")

  test("StreamingFormatConverterContext basic creation kafka to kafka ") {
    val configStr =
      s"""
         |input.format=kafka
         |input.kafka.bootstrap.servers=test_server_in
         |input.subscription.type="assign"
         |input.subscription.value="topic_in"
         |
         |output.format=kafka
         |output.kafka.bootstrap.servers=test_server_out
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expectedSource = KafkaStreamDataSourceConfiguration("test_server_in", KafkaSubscription("assign", "topic_in"))
    val expectedSink = KafkaStreamDataSinkConfiguration("test_server_out", GenericStreamDataSinkConfiguration(Kafka))
    val expected = StreamingFormatConverterContext(expectedSource, expectedSink)

    val result = StreamingFormatConverterContext(config)

    result.get shouldBe expected
  }

  test("StreamingFormatConverterContext basic creation kafka to file stream") {
    val configStr =
      s"""
         |input.format=kafka
         |input.kafka.bootstrap.servers=test_server_in
         |input.subscription.type="assign"
         |input.subscription.value="topic_in"
         |
         |output.format=json
         |output.path=my_path
         |output.options {
         |   key1: val1
         |   key2: val2
         |}
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expectedSource = KafkaStreamDataSourceConfiguration("test_server_in", KafkaSubscription("assign", "topic_in"))
    val expectedGenericSink = GenericStreamDataSinkConfiguration(Json, Map("key1" -> "val1", "key2" -> "val2"))
    val expectedSink = FileStreamDataSinkConfiguration("my_path", expectedGenericSink)
    val expected = StreamingFormatConverterContext(expectedSource, expectedSink)

    val result = StreamingFormatConverterContext(config)

    result.get shouldBe expected
  }

  test("StreamingFormatConverterContext basic creation file stream to kafka ") {
    val configStr =
      s"""
         |input.format=text
         |input.path="input_path"
         |
         |output.format=kafka
         |output.kafka.bootstrap.servers=test_server_out
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)

    val expectedSource = FileStreamDataSourceConfiguration("input_path", TextSourceConfiguration())
    val expectedSink = KafkaStreamDataSinkConfiguration("test_server_out", GenericStreamDataSinkConfiguration(Kafka))
    val expected = StreamingFormatConverterContext(expectedSource, expectedSink)

    val result = StreamingFormatConverterContext(config)

    result.get shouldBe expected
  }

}
