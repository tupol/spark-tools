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
import org.tupol.spark.io.implicits._
import org.tupol.spark.io.{ FormatAwareDataSinkConfiguration, FormatAwareDataSourceConfiguration }
import org.tupol.spark.SparkFun
import org.tupol.utils.implicits._

import scala.util.Try

/**
 * The SqlProcessor is a base class that can support multiple implementation, mainly designed to support registering
 * custom SQL functions (UDFs) to make them available while running the queries.
 */
abstract class SqlProcessor extends SparkFun[SqlProcessorContext, DataFrame](SqlProcessorContext.create) {

  def registerSqlFunctions(implicit spark: SparkSession, context: SqlProcessorContext): Unit

  override def run(implicit spark: SparkSession, context: SqlProcessorContext): Try[DataFrame] =
    for {
      _ <- Try(registerSqlFunctions)
            .logSuccess(_ => logInfo(s"Successfully registered the custom SQL functions."))
            .logFailure(logError(s"Failed to register the custom SQL functions.", _))
      _ <- context.inputTables.map {
            case (tableName, inputConfig) =>
              spark.source(inputConfig).read.map(_.createOrReplaceTempView(tableName))
          }.allOkOrFail
      sqlResult <- Try(spark.sql(context.renderedSql))
                    .logSuccess(_ => logInfo(s"Successfully ran the following query:\n${context.renderedSql}."))
                    .logFailure(logError(s"Failed to run the following query:\n${context.renderedSql}.", _))
      output <- sqlResult.sink(context.output).write
    } yield output

}

case class SqlProcessorContext(
  inputTables: Map[String, FormatAwareDataSourceConfiguration],
  output: FormatAwareDataSinkConfiguration,
  sql: Sql
) {
  def renderedSql: String = sql.render
}
object SqlProcessorContext {
  import org.tupol.spark.io.pureconf._
  import pureconfig.generic.auto._
  import org.tupol.spark.io.pureconf.readers._
  def create(config: Config): Try[SqlProcessorContext] = config.extract[SqlProcessorContext]
}
