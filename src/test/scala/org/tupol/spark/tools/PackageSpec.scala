package org.tupol.spark.tools

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PackageSpec extends AnyFunSuite with Matchers {


  test("replaceVariable returns the same text if no variables are defined") {

    val input = "SELECT {{columns}} FROM {{table.name}} WHERE {{condition_1}} AND a = '{{{condition-2}}}'"
    val result = replaceVariables(input, Map())

    result shouldBe input
  }

  test("replaceVariable returns the same text where only the defined variables are replaced") {

    val input = "SELECT {{columns}} FROM {{table.name}} WHERE {{condition_1}} AND a = '{{{condition-2}}}'"
    val result = replaceVariables(input, Map("columns" -> "a, b", "table.name" -> "some_table"))

    result shouldBe "SELECT a, b FROM some_table WHERE {{condition_1}} AND a = '{{{condition-2}}}'"
  }

  test("replaceVariable returns the text with all the variables replaced") {

    val input = "SELECT {{columns}} FROM {{table.name}} WHERE {{condition_1}} AND a = '{{{condition-2}}}'"
    val result = replaceVariables(
      input,
      Map("columns" -> "a, b", "table.name" -> "some_table", "condition_1" -> "b='x'", "condition-2" -> "why")
    )

    result shouldBe "SELECT a, b FROM some_table WHERE b='x' AND a = '{why}'"
  }

}
