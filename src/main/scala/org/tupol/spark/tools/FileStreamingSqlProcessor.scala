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
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.tupol.spark.implicits._
import org.tupol.spark.io.streaming.structured.{ FileStreamDataSinkConfiguration, FileStreamDataSourceConfiguration }
import org.tupol.spark.utils._
import org.tupol.spark.{ Logging, SparkApp }
import org.tupol.utils._
import org.tupol.utils.config.Configurator

import scala.util.Try

/**
 * The StreamingSqlProcessor is a base class that can support multiple implementation, mainly designed to support registering
 * custom SQL functions (UDFs) to make them available while running the queries.
 */
abstract class FileStreamingSqlProcessor extends SparkApp[FileStreamingSqlProcessorContext, DataFrame] {

  def registerSqlFunctions(implicit spark: SparkSession, context: FileStreamingSqlProcessorContext): Unit

  override def createContext(config: Config): FileStreamingSqlProcessorContext =
    FileStreamingSqlProcessorContext(config).get

  override def run(implicit spark: SparkSession, context: FileStreamingSqlProcessorContext): DataFrame =
    {
      Try(registerSqlFunctions)
        .logSuccess(_ => logInfo(s"Successfully registered the custom SQL functions."))
        .logFailure(logError(s"Failed to register the custom SQL functions.", _))
      context.inputTables.map {
        case (tableName, inputConfig) =>
          spark.source(inputConfig).read.createOrReplaceTempView(tableName)
      }
      val sqlResult = Try(spark.sql(context.renderedSql))
        .logSuccess(_ => logInfo(s"Successfully ran the following query:\n${context.renderedSql}."))
        .logFailure(logError(s"Failed to run the following query:\n${context.renderedSql}.", _))
        .get
      sqlResult.streamingSink(context.outputConfig).write.awaitTermination()
      sqlResult
    }

}

case class FileStreamingSqlProcessorContext(
  inputTables:    Map[String, FileStreamDataSourceConfiguration],
  inputVariables: Map[String, String],
  outputConfig:   FileStreamDataSinkConfiguration, sql: String
) {
  def renderedSql: String = replaceVariables(sql, inputVariables)
}

object FileStreamingSqlProcessorContext extends Configurator[FileStreamingSqlProcessorContext] with Logging {

  import com.typesafe.config.Config
  import org.tupol.utils.config._
  import scalaz.ValidationNel

  def validationNel(config: Config): ValidationNel[Throwable, FileStreamingSqlProcessorContext] = {
    import scalaz.syntax.applicative._

    val sql = config.extract[String]("input.sql.path").map { path =>
      fuzzyLoadTextResourceFile(path).getOrElse {
        logError("Failed loading the SQL query from the given path!")
        "UNABLE TO LOAD SQL FROM THE GIVEN PATH!"
      }
    }.orElse(config.extract[String]("input.sql.line"))

    config.extract[Map[String, FileStreamDataSourceConfiguration]]("input.tables") |@|
      config.extract[Option[Map[String, String]]]("input.variables").map(_.getOrElse(Map())) |@|
      config.extract[FileStreamDataSinkConfiguration]("output") |@|
      sql apply
      FileStreamingSqlProcessorContext.apply
  }

}
