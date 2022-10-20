package org.tupol.spark.tools

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Seconds, Span }
import org.scalatest.{ BeforeAndAfter, FunSuite, GivenWhenThen, Matchers }
import org.tupol.spark.SharedSparkSession
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.sources.JsonSourceConfiguration
import org.tupol.spark.io.streaming.structured._
import org.tupol.spark.sql._
import org.tupol.spark.testing.files.{ TestTempFilePath1, TestTempFilePath2 }

import scala.util.Random

class SimpleFileStreamingSqlProcessorSpec extends FunSuite
  with Matchers with GivenWhenThen with Eventually with BeforeAndAfter
  with SharedSparkSession with TestTempFilePath1 with TestTempFilePath2 {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  test("FileStreamingSqlProcessor SELECT *") {

    FileUtils.forceMkdir(testFile1)

    val inputConfig1 = FileStreamDataSourceConfiguration(
      testPath1,
      JsonSourceConfiguration(
        Map[String, String](),
        Some(schemaFor[TestValueRecord])
      )
    )
    val inputTables = Map("table1" -> inputConfig1)
    val testSQL = "SELECT * FROM table1"
    val genericSinkConfig = GenericStreamDataSinkConfiguration(FormatType.Json, Map(), None,
      Some(Trigger.ProcessingTime("1 second")))
    val sinkConfig = FileStreamDataSinkConfiguration(testPath2, genericSinkConfig, Some(testPath2))

    implicit val config = FileStreamingSqlProcessorContext(inputTables, Map(), sinkConfig, testSQL)

    val (streamingQuery, _) = SimpleFileStreamingSqlProcessor.run.get

    val testMessages = (1 to 4).map(i => f"""{"value": "test-message-$i%02d"} """)
    testMessages.foreach { message => addFile(message, testFile1) }

    eventually {
      val writtenData: DataFrame = spark.read.json(testPath2)
      writtenData.count() shouldBe 4
    }

    streamingQuery.stop
  }

  test("FileStreamingSqlProcessor SELECT * WHERE") {

    FileUtils.forceMkdir(testFile1)

    val inputConfig1 = FileStreamDataSourceConfiguration(
      testPath1,
      JsonSourceConfiguration(
        Map[String, String](),
        Some(schemaFor[TestValueRecord])
      )
    )
    val inputTables = Map("table1" -> inputConfig1)
    val testSQL = "SELECT * FROM table1 WHERE value LIKE 'test-%-03'"
    val genericSinkConfig = GenericStreamDataSinkConfiguration(FormatType.Json, Map(), None,
      Some(Trigger.ProcessingTime("1 second")))
    val sinkConfig = FileStreamDataSinkConfiguration(testPath2, genericSinkConfig, Some(testPath2))

    implicit val config = FileStreamingSqlProcessorContext(inputTables, Map(), sinkConfig, testSQL)

    val (streamingQuery, _) = SimpleFileStreamingSqlProcessor.run.get

    val testMessages = (1 to 4).map(i => f"""{"value": "test-message-$i%02d"} """)
    testMessages.foreach { message => addFile(message, testFile1) }

    eventually {
      val writtenData: DataFrame = spark.read.json(testPath2)
      writtenData.count() shouldBe 1
    }

    streamingQuery.stop
  }

  def addFile(text: String, parentFile: File): Unit = {
    val file = new File(parentFile, f"test-${math.abs(Random.nextLong())}%010d")
    FileUtils.write(file, text)
  }
}

case class TestValueRecord(value: String)
