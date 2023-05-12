package org.tupol.spark.tools

import java.io.File
import io.github.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ BeforeAndAfter, GivenWhenThen }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.streaming.structured._
import org.tupol.spark.testing._
import org.tupol.spark.testing.files.{ TestTempFilePath1, TestTempFilePath2 }

import java.nio.charset.Charset
import scala.util.Random

class StreamingStreamingFormatConverterSpec
    extends AnyFunSuite
    with Matchers
    with GivenWhenThen
    with Eventually
    with BeforeAndAfter
    with SharedSparkSession
    with EmbeddedKafka
    with TestTempFilePath1
    with TestTempFilePath2 {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  test("StreamingFormatConverter basic run test from parquet to json") {

    FileUtils.forceMkdir(testFile1)
    FileUtils.forceMkdir(testFile2)

    val options = Map[String, String]("path" -> testPath1)

    val inputConfig = GenericStreamDataSourceConfiguration(FormatType.Text, options, None)

    val genericSinkConfig = GenericStreamDataSinkConfiguration(
      FormatType.Json,
      Map(),
      Some("testQuery"),
      Some(Trigger.ProcessingTime("1 second"))
    )
    val sinkConfig = FileStreamDataSinkConfiguration(testPath2, genericSinkConfig, Some(testPath2))

    implicit val config = StreamingFormatConverterContext(inputConfig, sinkConfig)

    val streamingQuery = StreamingFormatConverter.run.get

    val testMessages = (1 to 4).map(i => f"test-message-$i%02d")

    testMessages.foreach { message =>
      addFile(message, testFile1)
    }

    val sourceData = spark.createDataFrame(testMessages.map(x => (x, x))).select("_1").toDF("value")

    eventually {
      val writtenData: DataFrame = spark.read.json(testPath2)
      writtenData.compareWith(sourceData).areEqual(false) shouldBe true
    }

    streamingQuery.stop
  }

  test("StreamingFormatConverter basic run test from kafka to json") {

    implicit val config = EmbeddedKafkaConfig()
    val topic           = "testTopic"
    val inputConfig = KafkaStreamDataSourceConfiguration(
      s":${config.kafkaPort}",
      KafkaSubscription("subscribe", topic),
      Some("earliest")
    )

    val genericSinkConfig = GenericStreamDataSinkConfiguration(
      FormatType.Json,
      Map(),
      Some("testQuery"),
      Some(Trigger.ProcessingTime("1 second"))
    )
    val sinkConfig = FileStreamDataSinkConfiguration(testPath2, genericSinkConfig, Some(testPath2))

    implicit val formatterConfig = StreamingFormatConverterContext(inputConfig, sinkConfig)

    withRunningKafka {

      val streamingQuery = StreamingFormatConverter.run.get

      val testMessages = (1 to 4).map(i => f"test-message-$i%02d")
      testMessages.foreach { message =>
        publishStringMessageToKafka(topic, message)
      }

      val sourceData = spark.createDataFrame(testMessages.map(x => (x, x))).select("_1").toDF("value")

      eventually {
        val writtenData: DataFrame = spark.read.json(testPath2)
        writtenData.select("value").compareWith(sourceData).areEqual(false) shouldBe true
      }
      streamingQuery.stop
    }

  }

  def addFile(text: String, parentFile: File): Unit = {
    val file = new File(parentFile, f"test-${math.abs(Random.nextLong())}%010d")
    FileUtils.write(file, text, Charset.defaultCharset)
  }
}
