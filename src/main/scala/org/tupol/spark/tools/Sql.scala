package org.tupol.spark.tools

import org.tupol.spark.utils.fuzzyLoadTextResourceFile
import org.tupol.utils.implicits._
import pureconfig.ConfigReader
import pureconfig.error.FailureReason

import scala.util.{ Failure, Success, Try }

case class Sql(sql: String, variables: Map[String, String] = Map()) {
  def render: String = replaceVariables(sql, variables)
}

object Sql {

  def fromLine(line: String, variables: Map[String, String] = Map()): Try[Sql] = from(None, Some(line), variables)
  def fromFile(path: String, variables: Map[String, String] = Map()): Try[Sql] = from(Some(path), None, variables)

  def from(path: Option[String], line: Option[String], variables: Map[String, String] = Map()): Try[Sql] =
    (path, line) match {
      case (None, None) =>
        Failure(new IllegalAccessException("Neither path nor line were defined; exactly one of them must be defined."))
      case (Some(_), Some(_)) =>
        Failure(new IllegalAccessException("Both path and line were defined; exactly one of them must be defined."))
      case (Some(path), None) =>
        fuzzyLoadTextResourceFile(path)
          .map(Sql(_, variables))
      case (None, Some(line)) =>
        Success(Sql(line, variables))
    }

  implicit val reader: ConfigReader[Sql] =
    ConfigReader
      .forProduct3[Try[Sql], Option[String], Option[String], Option[Map[String, String]]]("path", "line", "variables")(
        (path, line, options) => Sql.from(path, line, options.getOrElse(Map()))
      )
      .emap(
        sql =>
          sql.toEither.mapLeft(
            ex =>
              new FailureReason {
                override def description: String = ex.getMessage
              }
          )
      )

}
