package com.gsk.kg.engine.optimizer

import cats.syntax.either._
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.Path
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.sparqlparser.PropertyExpression.Reverse
import com.gsk.kg.sparqlparser.PropertyExpression.Uri
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils
import higherkindness.droste.data.Fix
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ReversePathRewriteSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TestUtils
    with TestConfig {

  type T = Fix[DAG]

  "ReversePathRewrite" should "exchange subject and object path triple" in {

    val q =
      """
        |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
        |
        |SELECT ?s ?o
        |WHERE {
        | ?s ^foaf:mbox ?o .
        |}
        |""".stripMargin

    parse(q, config)
      .map { case (query, _) =>
        val dag: T = DAG.fromQuery.apply(query)

        Fix.un(dag) match {
          case Project(_, Project(_, Path(s, expr, o, g))) =>
            s shouldEqual VARIABLE("?s")
            o shouldEqual VARIABLE("?o")
            expr shouldEqual Reverse(Uri("<http://xmlns.org/foaf/0.1/mbox>"))
          case _ =>
            fail
        }

        val reverse = ReversePathRewrite[T].apply(dag)
        Fix.un(reverse) match {
          case Project(_, Project(_, Path(s, expr, o, g))) =>
            s shouldEqual VARIABLE("?o")
            o shouldEqual VARIABLE("?s")
            expr shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
          case _ =>
            fail
        }
      }
      .getOrElse(fail)
  }
}
