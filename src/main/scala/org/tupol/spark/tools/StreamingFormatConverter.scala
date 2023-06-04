/*
MIT License

Copyright (c) 2018 Tupol (github.com/tupol)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
package org.tupol.spark.tools

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.tupol.spark.SparkApp
import org.tupol.spark.config.SimpleTypesafeConfigBuilder
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.streaming.structured._

import scala.util.Try

/**
 * Load a file into a [[org.apache.spark.sql.DataFrame]] and save it as a file in the specified path.
 *
 * <ul>
 *  <li>For general stream input configuration see more details here:
 *      [[https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources]]</li>>
 *  <li>For general stream output configuration see more details here:
 *      [[https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks]]</li>
 *  <li>For the XML converter see more details here:
 *      [[https://github.com/databricks/spark-xml]]</li>
 *  <li>For the CSV converter see more details here:
 *      [[https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html#csv(java.lang.String...)]]</li>
 *  <li>For the JSON converter see more details here:
 *      [[https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html#json(java.lang.String...)]]</li>
 *  <li>For the AVRO converter see more details here:
 *      [[https://github.com/databricks/spark-avro]]</li>
 * </ul>
 */
object StreamingFormatConverter extends SparkApp[StreamingFormatConverterContext, StreamingQuery] with SimpleTypesafeConfigBuilder {

  override def createContext(config: Config): Try[StreamingFormatConverterContext] =
    StreamingFormatConverterContext.create(config)

  override def run(implicit spark: SparkSession, context: StreamingFormatConverterContext): Try[StreamingQuery] =
    for {
      inputData <- context.input match {
                    case _: KafkaStreamDataSourceConfiguration =>
                      spark
                        .streamingSource(context.input)
                        .read
                        .map(
                          _.selectExpr(
                            "CAST(key AS STRING)",
                            "CAST(value AS STRING)",
                            "topic",
                            "partition",
                            "offset",
                            "timestamp",
                            "timestampType"
                          )
                        )
                    case _ => spark.streamingSource(context.input).read
                  }
      writeableData  = if (context.output.format == FormatType.Avro) inputData.makeAvroCompliant else inputData
      streamingQuery <- writeableData.streamingSink(context.output).write
      _              = sys.addShutdownHook(streamingQuery.awaitTermination(10000))
    } yield streamingQuery

}

/**
 * Context class for the [[StreamingFormatConverter]]
 *
 * @param input data source
 * @param output data sink
 */
case class StreamingFormatConverterContext(
  input: FormatAwareStreamingSourceConfiguration,
  output: FormatAwareStreamingSinkConfiguration
)

object StreamingFormatConverterContext {
  import org.tupol.spark.io.pureconf._
  import pureconfig.generic.auto._
  import org.tupol.spark.io.pureconf.streaming.structured.readers._
  def create(config: Config): Try[StreamingFormatConverterContext] = config.extract[StreamingFormatConverterContext]
}
