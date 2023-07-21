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
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.pureconf._
import org.tupol.spark.io.pureconf.readers._
import org.tupol.spark.io.streaming.structured.{FileStreamDataSinkConfiguration, FileStreamDataSourceConfiguration}
import org.tupol.spark.SparkApp
import org.tupol.utils.implicits._

import scala.util.Try

/**
 * The StreamingSqlProcessor is a base class that can support multiple implementation, mainly designed to
 * support registering custom SQL functions (UDFs) to make them available while running the queries.
 */
abstract class FileStreamingSqlProcessor
    extends SparkApp[FileStreamingSqlProcessorContext, (StreamingQuery, DataFrame)] {

  def registerSqlFunctions(implicit spark: SparkSession, context: FileStreamingSqlProcessorContext): Unit

  override def createContext(config: Config): Try[FileStreamingSqlProcessorContext] = {
    import pureconfig.generic.auto._
    config.extract[FileStreamingSqlProcessorContext]
  }

  override def run(
    implicit
    spark: SparkSession,
    context: FileStreamingSqlProcessorContext
  ): Try[(StreamingQuery, DataFrame)] =
    for {
      _ <- Try(registerSqlFunctions)
            .logSuccess(_ => logInfo(s"Successfully registered the custom SQL functions."))
            .logFailure(logError(s"Failed to register the custom SQL functions.", _))
      _ <- context.inputTables.map {
            case (tableName, inputConfig) =>
              spark.streamingSource(inputConfig).read.map(_.createOrReplaceTempView(tableName))
          }.allOkOrFail
      sqlResult <- Try(spark.sql(context.renderedSql))
                    .logSuccess(_ => logInfo(s"Successfully ran the following query:\n${context.renderedSql}"))
                    .logFailure(logError(s"Failed to run the following query:\n${context.renderedSql}", _))
      streamingQuery <- sqlResult.streamingSink(context.output).write
    } yield (streamingQuery, sqlResult)

}

case class FileStreamingSqlProcessorContext(
  inputTables: Map[String, FileStreamDataSourceConfiguration],
  output: FileStreamDataSinkConfiguration,
  sql: Sql
) {
  def renderedSql: String = sql.render
}
