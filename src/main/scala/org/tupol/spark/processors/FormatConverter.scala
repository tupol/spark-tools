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
package org.tupol.spark.processors

import com.typesafe.config.Config
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.tupol.spark.SparkRunnable
import org.tupol.spark.implicits._
import org.tupol.spark.io._
import org.tupol.utils.config.Configurator

import scala.util.Try

/**
 * Load a file into a DataFrame and save it as a file in the specified path.
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
 * </ul>
 */
object FormatConverter extends SparkRunnable[FormatConverterConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration], DataFrame] {

  override def buildConfig(config: Config): Try[FormatConverterConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]] = FormatConverterConfig(config)

  override def run(implicit spark: SparkSession, config: FormatConverterConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]): Try[DataFrame] =
    for {
      inputData <- spark.source(config.input).read
      writeableData = if (config.output.format == FormatType.Avro) inputData.makeAvroCompliant else inputData
      writtenData <- writeableData.sink(config.output).write
    } yield writtenData

}

/**
 * Configuration class for the [[FormatConverter]]
 *
 * @param input
 * @param output
 */
case class FormatConverterConfig[SourceConfig <: FormatAwareDataSourceConfiguration, SinkConfig <: FormatAwareDataSinkConfiguration](val input: SourceConfig, val output: SinkConfig)

object FormatConverterConfig extends Configurator[FormatConverterConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]] {

  import com.typesafe.config.Config
  import scalaz.ValidationNel

  def validationNel(config: Config): ValidationNel[Throwable, FormatConverterConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]] = {
    import org.tupol.utils.config._
    import scalaz.syntax.applicative._

    config.extract[FormatAwareDataSourceConfiguration]("input") |@|
      config.extract[FormatAwareDataSinkConfiguration]("output") apply
      FormatConverterConfig.apply
  }
}
