package com.gsk.kg.engine.typed.functions

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators
import com.gsk.kg.engine.syntax._
import com.gsk.kg.engine.typed.functions.TypedLiterals.DateLiteral

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class TypedLiteralsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "TypedLiterals" when {

    "DateLiteral" when {

      "parseDateFromRDFDateTime" should {

        "work for all types of dates specified by RDF spec" in {

          val df = List(
            """"2001-10-26T21:32:52"^^<http://www.w3.org/2001/XMLSchema#datetime>""",
            """"2001-10-26T21:32:52+02:00"^^<http://www.w3.org/2001/XMLSchema#datetime>""",
            """"2001-10-26T19:32:52Z"^^<http://www.w3.org/2001/XMLSchema#datetime>""",
            """"2001-10-26T19:32:52+00:00"^^<http://www.w3.org/2001/XMLSchema#datetime>""",
            """"2001-10-26T21:32:52.12679"^^<http://www.w3.org/2001/XMLSchema#datetime>"""
          ).toTypedDF("date")

          val results = df
            .select(DateLiteral.parseDateFromRDFDateTime(df("date")))
            .collect()

          results
            .map(_.get(0)) shouldNot contain(null)
        }
      }
    }
  }
}
