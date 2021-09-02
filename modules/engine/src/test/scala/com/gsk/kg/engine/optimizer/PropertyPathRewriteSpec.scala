package com.gsk.kg.engine.optimizer

import cats.syntax.either._
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.Join
import com.gsk.kg.engine.DAG.Path
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.engine.DAG.Union
import com.gsk.kg.engine.data.ToTree.ToTreeOps
import com.gsk.kg.sparqlparser.PropertyExpression.Reverse
import com.gsk.kg.sparqlparser.PropertyExpression.Uri
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils
import higherkindness.droste.data.Fix
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class PropertyPathRewriteSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with TestUtils
    with TestConfig {

  type T = Fix[DAG]

  "PropertyPathRewrite" should {

    "rewrite DAG" when {

      "^uri" in {

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
            val dag: T  = DAG.fromQuery.apply(query)
            val reverse = PropertyPathRewrite[T].apply(dag)

            Fix.un(reverse) match {
              case Project(_, Project(_, Path(s, expr, o, g))) =>
                s shouldEqual VARIABLE("?o")
                o shouldEqual VARIABLE("?s")
                expr shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
              case _ =>
                fail
            }
          }
      }

      "sequence" when {

        "(uri1/uri2/uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows/foaf:name/foaf:mbox ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              import cats.instances.string._
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              println(dag.toTree.drawTree)
              println(reverse.toTree.drawTree)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Join(
                          Join(
                            Path(sll, ell, oll, gll),
                            Path(slr, elr, olr, glr)
                          ),
                          Path(sr, er, or, gr)
                        )
                      )
                    ) =>
//                  sll shouldEqual VARIABLE("?s")
//                  sr shouldEqual VARIABLE("?s")
//                  oll shouldEqual slr
//                  olr shouldEqual VARIABLE("?o")
//                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "^(uri1/uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:name/foaf:mbox) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Join(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?o")
                  sr shouldEqual ol
                  or shouldEqual VARIABLE("?s")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }

        "(^ur1/uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:name/foaf:mbox) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Join(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual sr
                  ol shouldEqual VARIABLE("?s")
                  or shouldEqual VARIABLE("?o")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }

        "(ur1/^uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:name/^foaf:mbox) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Join(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?o")
                  ol shouldEqual or
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }

        "(^ur1/^uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:name/^foaf:mbox) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Join(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?o")
                  ol shouldEqual sr
                  or shouldEqual VARIABLE("?s")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }
      }

      "alternative" when {

        "(uri1|uri2|uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows|foaf:name|foaf:mbox ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              import cats.instances.string._
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              println(dag.toTree.drawTree)
              println(reverse.toTree.drawTree)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Union(
                          Union(
                            Path(sll, ell, oll, gll),
                            Path(slr, elr, olr, glr)
                          ),
                          Path(sr, er, or, gr)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  slr shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  oll shouldEqual VARIABLE("?o")
                  olr shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "^(uri1|uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:name|foaf:mbox|foaf:knows) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Union(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?o")
                  sr shouldEqual VARIABLE("?o")
                  ol shouldEqual VARIABLE("?s")
                  or shouldEqual VARIABLE("?s")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }

        "(^ur1|uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:name|foaf:mbox) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Union(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?o")
                  sr shouldEqual VARIABLE("?s")
                  ol shouldEqual VARIABLE("?s")
                  or shouldEqual VARIABLE("?o")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }

        }

        "(ur1|^uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:name|^foaf:mbox) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Union(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?o")
                  ol shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?s")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }

        "(^ur1|^uri2)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:name|^foaf:mbox) ?o .
              |}
              |""".stripMargin

          parse(q, config)
            .map { case (query, _) =>
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Union(Path(sl, el, ol, gl), Path(sr, er, or, gr))
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?o")
                  sr shouldEqual VARIABLE("?o")
                  ol shouldEqual VARIABLE("?s")
                  or shouldEqual VARIABLE("?s")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }
      }
    }
  }
}
