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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tupol.spark.SparkFun
import org.tupol.spark.io._
import org.tupol.spark.io.implicits._

import scala.util.Try

/**
 * Load a file into a [[DataFrame]] and save it as a file in the specified path.
 *
 * <ul>
 *  <li>For the XML converter see more details here:
 *      [[https://github.com/databricks/spark-xml]]</li>
 *  <li>For the CSV converter see more details here:
 *      [[https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html#csv(java.lang.String...)]]</li>
 *  <li>For the JSON converter see more details here:
 *      [[https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/sql/DataFrameReader.html#json(java.lang.String...)]]</li>
 *  <li>For the AVRO converter see more details here:
 *      [[https://github.com/databricks/spark-avro]]</li>
 *  <li>For the DELTA converter see more details here:
 *      [[https://github.com/delta-io/delta]]</li>
 * </ul>
 */
object FormatConverter extends SparkFun[FormatConverterContext, DataFrame](FormatConverterContext.create(_)) {

  override def run(implicit spark: SparkSession, context: FormatConverterContext): Try[DataFrame] =
    for {
      inputData <- spark.source(context.input).read
      writeableData = if (context.output.format == FormatType.Avro) inputData.makeAvroCompliant else inputData
      output <- writeableData.sink(context.output).write
    } yield output

}

/**
 * Context class for the [[FormatConverter]]
 *
 * @param input data source
 * @param output data sink
 */
case class FormatConverterContext(input: FormatAwareDataSourceConfiguration, output: FormatAwareDataSinkConfiguration)

object FormatConverterContext {
  import org.tupol.spark.io.pureconf._
  import pureconfig.generic.auto._
  import org.tupol.spark.io.pureconf.readers._
  def create(config: Config): Try[FormatConverterContext] =
    config.extract[FormatConverterContext]
}
