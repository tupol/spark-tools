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
import org.tupol.spark.io.{ FileDataFrameLoaderConfig, FileDataFrameSaverConfig }
import org.tupol.spark.utils._
import org.tupol.spark.{ Logging, SparkRunnable }
import org.tupol.utils._
import org.tupol.utils.config.Configurator

import scala.util.Try

/**
 * The SqlProcessor is a base class that can support multiple implementation, mainly designed to support registering
 * custom SQL functions (UDFs) to make them available while running the queries.
 */
abstract class SqlProcessor extends SparkRunnable[SqlProcessorConfig, DataFrame] {

  def registerSqlFunctions: Try[Unit]

  override def buildConfig(config: Config): Try[SqlProcessorConfig] = SqlProcessorConfig(config)

  override def run(implicit spark: SparkSession, config: SqlProcessorConfig): Try[DataFrame] =
    for {
      _ <- registerSqlFunctions
        .logSuccess(_ => logInfo(s"Successfully registered the custom SQL functions."))
        .logFailure(logError(s"Failed to register the custom SQL functions.", _))

      _ <- config.inputTables.map {
        case (tableName, inputConfig) =>
          spark.loadData(inputConfig).map(_.createOrReplaceTempView(tableName))
            .logSuccess(_ => logInfo(s"Successfully loaded '${inputConfig.path}' as '${inputConfig.format}' into the '$tableName' table."))
            .logFailure(logError(s"Failed to load '${inputConfig.path}' as '${inputConfig.format}' into the '$tableName' table.", _))
      }.toSeq.allOkOrFail

      sqlResult <- Try(spark.sql(config.renderedSql))
        .logSuccess(_ => logInfo(s"Successfully ran the following query:\n${config.renderedSql}."))
        .logFailure(logError(s"Failed to run the following query:\n${config.renderedSql}.", _))

      writtenResult <- sqlResult.saveData(config.outputConfig)
        .logSuccess(_ => logInfo(s"Successfully written the query results to '${config.outputConfig.path}'." +
          s"as '${config.outputConfig.format}'."))
        .logFailure(logError(s"Failed to write the query results to '${config.outputConfig.path}'." +
          s"as '${config.outputConfig.format}'.", _))
    } yield writtenResult

}

case class SqlProcessorConfig(inputTables: Map[String, FileDataFrameLoaderConfig], inputVariables: Map[String, String],
  outputConfig: FileDataFrameSaverConfig, sql: String) {

  def renderedSql: String = SqlProcessorConfig.replaceVariables(sql, inputVariables)
}

object SqlProcessorConfig extends Configurator[SqlProcessorConfig] with Logging {

  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel

  def apply(inputTablePaths: Map[String, FileDataFrameLoaderConfig], outputConfig: FileDataFrameSaverConfig,
    sql: String): SqlProcessorConfig = SqlProcessorConfig(inputTablePaths, Map(), outputConfig, sql)

  def validationNel(config: Config): ValidationNel[Throwable, SqlProcessorConfig] = {
    import scalaz.syntax.applicative._

    val sql = config.extract[String]("input.sql.path").map { path =>
      fuzzyLoadTextResourceFile(path).getOrElse {
        logError("Failed loading the SQL query from the given path!")
        "UNABLE TO LOAD SQL FROM THE GIVEN PATH!"
      }
    }.orElse(config.extract[String]("input.sql.line"))

    config.extract[Map[String, FileDataFrameLoaderConfig]]("input.tables") |@|
      config.extract[Option[Map[String, String]]]("input.variables").map(_.getOrElse(Map())) |@|
      config.extract[FileDataFrameSaverConfig]("output") |@|
      sql apply
      SqlProcessorConfig.apply
  }

  def replaceVariables(text: String, variables: Map[String, String]): String = {
    def renderVariableName(name: String) = s"\\{\\{${name.trim}\\}\\}"
    variables.foldLeft(text) { case (newText, (k, v)) => newText.replaceAll(renderVariableName(k), v) }
  }

}
