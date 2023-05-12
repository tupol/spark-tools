package org.tupol.spark.tools

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{ Failure, Success }

class SqlSpec extends AnyFlatSpec with Matchers {

  "Sql.from()" should "succeed when path is specified" in {
    val expected = Sql("""-- Some comment
                         |SELECT *
                         |-- Some other comment
                         |FROM table1
                         |WHERE
                         |-- Some filter comment
                         |table1.id="1001"""".stripMargin)
    Sql.from(Some("src/test/resources/SqlProcessor/test.sql"), None) shouldBe Success(expected)
  }
  it should "succeed when line is specified" in {
    val expected = Sql("SELECT * FROM table1 where table1.id=\"1002\"")
    Sql.from(None, Some("SELECT * FROM table1 where table1.id=\"1002\"")) shouldBe Success(expected)
  }
  it should "fail when both path and line are defined" in {
    Sql.from(Some("path"), Some("line")) shouldBe a[Failure[_]]
  }

}
