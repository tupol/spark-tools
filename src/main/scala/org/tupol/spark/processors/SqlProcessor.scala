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
import org.tupol.spark.implicits._
import org.tupol.spark.io.{ FormatAwareDataSinkConfiguration, FormatAwareDataSourceConfiguration }
import org.tupol.spark.utils._
import org.tupol.spark.{ Logging, SparkApp }
import org.tupol.utils._
import org.tupol.utils.config.Configurator

import scala.util.Try

/**
 * The SqlProcessor is a base class that can support multiple implementation, mainly designed to support registering
 * custom SQL functions (UDFs) to make them available while running the queries.
 */
abstract class SqlProcessor extends SparkApp[SqlProcessorConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration], DataFrame] {

  def registerSqlFunctions: Try[Unit]

  override def buildConfig(config: Config): Try[SqlProcessorConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]] = SqlProcessorConfig(config)

  override def run(implicit spark: SparkSession, config: SqlProcessorConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]): Try[DataFrame] =
    for {
      _ <- registerSqlFunctions
        .logSuccess(_ => logInfo(s"Successfully registered the custom SQL functions."))
        .logFailure(logError(s"Failed to register the custom SQL functions.", _))
      _ <- config.inputTables.map {
        case (tableName, inputConfig) =>
          spark.source(inputConfig).read.map(_.createOrReplaceTempView(tableName))
      }.toSeq.allOkOrFail
      sqlResult <- Try(spark.sql(config.renderedSql))
        .logSuccess(_ => logInfo(s"Successfully ran the following query:\n${config.renderedSql}."))
        .logFailure(logError(s"Failed to run the following query:\n${config.renderedSql}.", _))
      writtenResult <- sqlResult.sink(config.outputConfig).write
    } yield writtenResult

}

case class SqlProcessorConfig[SourceConfig <: FormatAwareDataSourceConfiguration, SinkConfig <: FormatAwareDataSinkConfiguration](inputTables: Map[String, SourceConfig], inputVariables: Map[String, String], outputConfig: SinkConfig, sql: String) {

  def renderedSql: String = SqlProcessorConfig.replaceVariables(sql, inputVariables)
}

object SqlProcessorConfig extends Configurator[SqlProcessorConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]] with Logging {

  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel

  def validationNel(config: Config): ValidationNel[Throwable, SqlProcessorConfig[_ <: FormatAwareDataSourceConfiguration, _ <: FormatAwareDataSinkConfiguration]] = {
    import scalaz.syntax.applicative._

    val sql = config.extract[String]("input.sql.path").map { path =>
      fuzzyLoadTextResourceFile(path).getOrElse {
        logError("Failed loading the SQL query from the given path!")
        "UNABLE TO LOAD SQL FROM THE GIVEN PATH!"
      }
    }.orElse(config.extract[String]("input.sql.line"))

    config.extract[Map[String, FormatAwareDataSourceConfiguration]]("input.tables") |@|
      config.extract[Option[Map[String, String]]]("input.variables").map(_.getOrElse(Map())) |@|
      config.extract[FormatAwareDataSinkConfiguration]("output") |@|
      sql apply
      SqlProcessorConfig.apply
  }

  def replaceVariables(text: String, variables: Map[String, String]): String = {
    def renderVariableName(name: String) = s"\\{\\{${name.trim}\\}\\}"
    variables.foldLeft(text) { case (newText, (k, v)) => newText.replaceAll(renderVariableName(k), v) }
  }

}
