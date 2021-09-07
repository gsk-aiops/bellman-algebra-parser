package com.gsk.kg.engine.optimizer

import cats.syntax.either._

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.Join
import com.gsk.kg.engine.DAG.Path
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.engine.DAG.Union
import com.gsk.kg.sparqlparser.PropertyExpression.Uri
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils

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
              case Project(_, Project(_, Path(s, expr, o, g, true))) =>
                s shouldEqual VARIABLE("?s")
                o shouldEqual VARIABLE("?o")
                expr shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
              case _ =>
                fail
            }
          }
      }

      "sequence" when {

        "(uri1/uri2/uri3/uri4)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows/foaf:name/foaf:mbox/foaf:surname ?o .
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
                        Join(
                          Join(
                            Join(
                              Path(slll, elll, olll, glll, false),
                              Path(sllr, ellr, ollr, gllr, false)
                            ),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  slll shouldEqual VARIABLE("?s")
                  olll shouldEqual sllr
                  ollr shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  elll shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  ellr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/surname>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "^(uri1/uri2/uri3/uri4)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows/foaf:name/foaf:mbox/foaf:surname) ?o .
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
                        Join(
                          Join(
                            Join(
                              Path(slll, elll, olll, glll, true),
                              Path(sllr, ellr, ollr, gllr, true)
                            ),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  slll shouldEqual VARIABLE("?s")
                  olll shouldEqual sllr
                  ollr shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  elll shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  ellr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/surname>")
                case x =>
                  fail(x.toString)
              }
            }
        }

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
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "(^uri1/uri2/uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows/foaf:name/foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "(uri1/^uri2/uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/^foaf:name/foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "(uri1/uri2/^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/foaf:name/^foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "(^uri1/^uri2/uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows/^foaf:name/foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "(^uri1/uri2/^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows/foaf:name/^foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "(uri1/^uri2/^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows/^foaf:name/^foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "(^uri1/^uri2/^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows/^foaf:name/^foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  ell shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "^(uri1/uri2/uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows/foaf:name/foaf:mbox) ?o .
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
                        Join(
                          Join(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sll shouldEqual VARIABLE("?s")
                  oll shouldEqual slr
                  olr shouldEqual sr
                  or shouldEqual VARIABLE("?o")
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
                        Join(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual ol
                  or shouldEqual VARIABLE("?o")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }

        "(^uri1/^uri2)" in {

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
                        Join(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual ol
                  or shouldEqual VARIABLE("?o")
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
                        Join(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  ol shouldEqual sr
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
                        Join(
                          Path(sl, el, ol, gl, false),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  ol shouldEqual sr
                  or shouldEqual VARIABLE("?o")
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
                        Join(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  ol shouldEqual sr
                  or shouldEqual VARIABLE("?o")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }
      }

      "alternative" when {

        "(uri1|uri2|uri3|uri4)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows|foaf:name|foaf:mbox|foaf:surname ?o .
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
                        Union(
                          Union(
                            Union(
                              Path(slll, elll, olll, glll, false),
                              Path(sllr, ellr, ollr, gllr, false)
                            ),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  slll shouldEqual VARIABLE("?s")
                  sllr shouldEqual VARIABLE("?s")
                  slr shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  olll shouldEqual VARIABLE("?o")
                  ollr shouldEqual VARIABLE("?o")
                  olr shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?o")
                  elll shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  ellr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/surname>")
                case x =>
                  fail(x.toString)
              }
            }
        }

        "^(uri1|uri2|uri3|uri4)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows|foaf:name|foaf:mbox|foaf:surname) ?o .
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
                        Union(
                          Union(
                            Union(
                              Path(slll, elll, olll, glll, true),
                              Path(sllr, ellr, ollr, gllr, true)
                            ),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  slll shouldEqual VARIABLE("?s")
                  sllr shouldEqual VARIABLE("?s")
                  slr shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  olll shouldEqual VARIABLE("?o")
                  ollr shouldEqual VARIABLE("?o")
                  olr shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?o")
                  elll shouldEqual Uri("<http://xmlns.org/foaf/0.1/knows>")
                  ellr shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  elr shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/surname>")
                case x =>
                  fail(x.toString)
              }
            }
        }

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
              val dag: T  = DAG.fromQuery.apply(query)
              val reverse = PropertyPathRewrite[T].apply(dag)

              Fix.un(reverse) match {
                case Project(
                      _,
                      Project(
                        _,
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, false)
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

        "(^uri1|uri2|uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows|foaf:name|foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, false)
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

        "(uri1|^uri2|uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|^foaf:name|foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, false)
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

        "(uri1|uri2|^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|foaf:name|^foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, true)
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

        "(^uri1|^uri2|uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows|^foaf:name|foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, false)
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

        "(^uri1|uri2|^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows|foaf:name|^foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, false)
                          ),
                          Path(sr, er, or, gr, true)
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

        "(uri1|^uri2|^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|^foaf:name|^foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, false),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
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

        "(^uri1|^uri2|^uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows|^foaf:name|^foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
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

        "^(uri1|uri2|uri3)" in {

          val q =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows|foaf:name|foaf:mbox) ?o .
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
                        Union(
                          Union(
                            Path(sll, ell, oll, gll, true),
                            Path(slr, elr, olr, glr, true)
                          ),
                          Path(sr, er, or, gr, true)
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
              | ?s ^(foaf:name|foaf:mbox) ?o .
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
                        Union(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  ol shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?o")
                  el shouldEqual Uri("<http://xmlns.org/foaf/0.1/name>")
                  er shouldEqual Uri("<http://xmlns.org/foaf/0.1/mbox>")
                case _ =>
                  fail
              }
            }
        }

        "(^uri1|^uri2)" in {

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
                        Union(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  ol shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?o")
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
                        Union(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, false)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  ol shouldEqual VARIABLE("?o")
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
                        Union(
                          Path(sl, el, ol, gl, false),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  ol shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?o")
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
                        Union(
                          Path(sl, el, ol, gl, true),
                          Path(sr, er, or, gr, true)
                        )
                      )
                    ) =>
                  sl shouldEqual VARIABLE("?s")
                  sr shouldEqual VARIABLE("?s")
                  ol shouldEqual VARIABLE("?o")
                  or shouldEqual VARIABLE("?o")
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
