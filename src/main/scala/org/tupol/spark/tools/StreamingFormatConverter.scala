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
import org.tupol.spark.implicits._
import org.tupol.spark.io.FormatType
import org.tupol.spark.io.streaming.structured._
import org.tupol.utils.config.Configurator

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
object StreamingFormatConverter extends SparkApp[StreamingFormatConverterContext, StreamingQuery] {

  override def createContext(config: Config): StreamingFormatConverterContext = StreamingFormatConverterContext(config).get

  override def run(implicit spark: SparkSession, context: StreamingFormatConverterContext): StreamingQuery = {
    val inputData = context.input match {
      case _: KafkaStreamDataSourceConfiguration =>
        spark.source(context.input).read.selectExpr(
          "CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset",
          "timestamp", "timestampType"
        )
      case _ => spark.source(context.input).read
    }
    val writeableData = if (context.output.format == FormatType.Avro) inputData.makeAvroCompliant else inputData
    val streamingQuery: StreamingQuery = writeableData.streamingSink(context.output).write
    sys.addShutdownHook(streamingQuery.awaitTermination(10000))
    streamingQuery
  }
}

/**
 * Context class for the [[StreamingFormatConverter]]
 *
 * @param input data source
 * @param output data sink
 */
case class StreamingFormatConverterContext(input: FormatAwareStreamingSourceConfiguration, output: FormatAwareStreamingSinkConfiguration)

object StreamingFormatConverterContext extends Configurator[StreamingFormatConverterContext] {

  import scalaz.ValidationNel

  def validationNel(config: Config): ValidationNel[Throwable, StreamingFormatConverterContext] = {
    import org.tupol.utils.config._
    import scalaz.syntax.applicative._

    config.extract[FormatAwareStreamingSourceConfiguration]("input") |@|
      config.extract[FormatAwareStreamingSinkConfiguration]("output") apply
      StreamingFormatConverterContext.apply
  }
}
