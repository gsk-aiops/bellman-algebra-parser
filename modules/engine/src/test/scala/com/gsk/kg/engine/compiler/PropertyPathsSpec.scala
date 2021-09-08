package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PropertyPathsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "Property Paths" should {

    "perform on simple queries" when {

      "alternative | property path" when {

        "no reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s foaf:knows|foaf:name ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSet shouldEqual Set(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "left reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (^foaf:knows|foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSet shouldEqual Set(
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("<http://example.org/Charles>", "\"Charles\"")
          )
        }

        "right reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s (foaf:knows|^foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSet shouldEqual Set(
            Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
            Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
            Row("\"Charles\"", "<http://example.org/Charles>")
          )
        }

        "both reversed" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s ^(foaf:knows|foaf:name) ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSet shouldEqual Set(
            Row("<http://example.org/Bob>", "<http://example.org/Alice>"),
            Row("<http://example.org/Charles>", "<http://example.org/Bob>"),
            Row("\"Charles\"", "<http://example.org/Charles>")
          )
        }
      }

      "sequence / property path" when {

        "two seq URIs chained" when {

          "no reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s foaf:knows/foaf:name ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Bob>", "\"Charles\"")
            )
          }

          "first reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Daniel>", "\"Charles\"")
            )
          }

          "second reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:knows) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
              Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
              Row(
                "<http://example.org/Charles>",
                "<http://example.org/Charles>"
              )
            )
          }

          "all reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Bob>", "\"Charles\""),
              Row("<http://example.org/Daniel>", "\"Charles\"")
            )
          }
        }

        "three seq URIs chained" when {

          "no reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s foaf:knows/foaf:friend/foaf:name ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "first reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "second reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "first and second reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/name>",
                "\"Charles\""
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "first and third reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "second and third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/^foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }

          "all reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "\"Charles\"",
                "<http://xmlns.org/foaf/0.1/name>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s ^(foaf:knows/foaf:friend/foaf:name) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "\"Charles\"")
            )
          }
        }

        "four seq URIs chained" when {

          "no reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s foaf:knows/foaf:friend/foaf:parent/foaf:employee ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first and second reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first and third reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second and third reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "third and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first, second and third reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Eduard>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/^foaf:parent/foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first, second and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Daniel>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "first, third and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "second, third and fourth reversed" in {

            val df = List(
              (
                "<http://example.org/Alice>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (foaf:knows/^foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }

          "all reversed" in {

            val df = List(
              (
                "<http://example.org/Bob>",
                "<http://xmlns.org/foaf/0.1/knows>",
                "<http://example.org/Alice>"
              ),
              (
                "<http://example.org/Charles>",
                "<http://xmlns.org/foaf/0.1/friend>",
                "<http://example.org/Bob>"
              ),
              (
                "<http://example.org/Daniel>",
                "<http://xmlns.org/foaf/0.1/parent>",
                "<http://example.org/Charles>"
              ),
              (
                "<http://example.org/Eduard>",
                "<http://xmlns.org/foaf/0.1/employee>",
                "<http://example.org/Daniel>"
              )
            ).toDF("s", "p", "o")

            val query =
              """
                |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
                |
                |SELECT ?s ?o
                |WHERE {
                | ?s (^foaf:knows/^foaf:friend/^foaf:parent/^foaf:employee) ?o .
                |}
                |""".stripMargin

            val result = Compiler.compile(df, query, config)

            result.right.get.collect.toSet shouldEqual Set(
              Row("<http://example.org/Alice>", "<http://example.org/Eduard>")
            )
          }
        }
      }

      "reverse ^ property path" in {

        val df = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.org/foaf/0.1/mbox>",
            "<mailto:alice@example.org>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?o ?s
            |WHERE {
            | ?o ^foaf:mbox ?s .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<mailto:alice@example.org>", "<http://example.org/alice>")
        )
      }

      "arbitrary length + property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows+ ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
          Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
          Row("<http://example.org/Bob>", "<http://example.org/Charles>")
        )
      }

      "arbitrary length * property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows* ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Charles\"", "\"Charles\""),
          Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
          Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
          Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
          Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
          Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
          Row("<http://example.org/Alice>", "<http://example.org/Charles>")
        )
      }

      "optional ? property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows? ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Charles\"", "\"Charles\""),
          Row("<http://example.org/Charles>", "<http://example.org/Charles>"),
          Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
          Row("<http://example.org/Bob>", "<http://example.org/Bob>"),
          Row("<http://example.org/Alice>", "<http://example.org/Alice>"),
          Row("<http://example.org/Alice>", "<http://example.org/Bob>")
        )
      }

      "negated ! property path" when {

        "simple uri predicate" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:knows ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Charles>",
              "\"Charles\""
            )
          )
        }

        "nested property path" in {

          val df = List(
            (
              "<http://example.org/Alice>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Bob>"
            ),
            (
              "<http://example.org/Bob>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Charles>"
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/name>",
              "\"Charles\""
            ),
            (
              "<http://example.org/Charles>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Daniel>"
            ),
            (
              "<http://example.org/Daniel>",
              "<http://xmlns.org/foaf/0.1/knows>",
              "<http://example.org/Erick>"
            )
          ).toDF("s", "p", "o")

          val query =
            """
              |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
              |
              |SELECT ?s ?o
              |WHERE {
              | ?s !foaf:name{2} ?o .
              |}
              |""".stripMargin

          val result = Compiler.compile(df, query, config)

          result.right.get.collect.toSet shouldEqual Set(
            Row(
              "<http://example.org/Charles>",
              "<http://example.org/Erick>"
            ),
            Row(
              "<http://example.org/Bob>",
              "<http://example.org/Daniel>"
            ),
            Row(
              "<http://example.org/Alice>",
              "<http://example.org/Charles>"
            )
          )
        }
      }

      "fixed length {n,m} property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Daniel>"
          ),
          (
            "<http://example.org/Daniel>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Erick>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows{1, 3} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
          Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
          Row("<http://example.org/Alice>", "<http://example.org/Daniel>"),
          Row("<http://example.org/Bob>", "<http://example.org/Charles>"),
          Row("<http://example.org/Bob>", "<http://example.org/Daniel>"),
          Row("<http://example.org/Bob>", "<http://example.org/Erick>"),
          Row("<http://example.org/Charles>", "<http://example.org/Daniel>"),
          Row("<http://example.org/Charles>", "<http://example.org/Erick>"),
          Row("<http://example.org/Daniel>", "<http://example.org/Erick>")
        )
      }

      "fixed length {n,} property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Daniel>"
          ),
          (
            "<http://example.org/Daniel>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Erick>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows{2,} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "<http://example.org/Alice>",
            "<http://example.org/Charles>"
          ),
          Row(
            "<http://example.org/Alice>",
            "<http://example.org/Daniel>"
          ),
          Row(
            "<http://example.org/Alice>",
            "<http://example.org/Erick>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://example.org/Daniel>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://example.org/Erick>"
          ),
          Row(
            "<http://example.org/Charles>",
            "<http://example.org/Erick>"
          )
        )
      }

      "fixed length {,n} property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Daniel>"
          ),
          (
            "<http://example.org/Daniel>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Erick>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows{,2} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "\"Charles\"",
            "\"Charles\""
          ),
          Row(
            "<http://example.org/Alice>",
            "<http://example.org/Alice>"
          ),
          Row(
            "<http://example.org/Alice>",
            "<http://example.org/Bob>"
          ),
          Row(
            "<http://example.org/Alice>",
            "<http://example.org/Charles>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://example.org/Bob>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://example.org/Charles>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://example.org/Daniel>"
          ),
          Row(
            "<http://example.org/Charles>",
            "<http://example.org/Charles>"
          ),
          Row(
            "<http://example.org/Charles>",
            "<http://example.org/Daniel>"
          ),
          Row(
            "<http://example.org/Charles>",
            "<http://example.org/Erick>"
          ),
          Row(
            "<http://example.org/Daniel>",
            "<http://example.org/Daniel>"
          ),
          Row(
            "<http://example.org/Daniel>",
            "<http://example.org/Erick>"
          ),
          Row(
            "<http://example.org/Erick>",
            "<http://example.org/Erick>"
          )
        )
      }

      "fixed length {n} property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Daniel>"
          ),
          (
            "<http://example.org/Daniel>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Erick>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows{2} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/Alice>",
            "<http://example.org/Charles>"
          ),
          Row(
            "<http://example.org/Bob>",
            "<http://example.org/Daniel>"
          ),
          Row(
            "<http://example.org/Charles>",
            "<http://example.org/Erick>"
          )
        )
      }
    }

    "perform on complex queries" when {

      "complex query 1" ignore {

        val df = List(
          (
            "<http://example.org/1>",
            "<http://example.org/a>",
            "<http://example.org/2>"
          ),
          (
            "<http://example.org/2>",
            "<http://example.org/b>",
            "<http://example.org/3>"
          ),
          (
            "<http://example.org/3>",
            "<http://example.org/c>",
            "<http://example.org/4>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s ^!^ex:a*/ex:b?|ex:c+ ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "complex query 2" ignore {

        val df = List(
          (
            "<http://example.org/1>",
            "<http://example.org/a>",
            "<http://example.org/2>"
          ),
          (
            "<http://example.org/2>",
            "<http://example.org/b>",
            "<http://example.org/3>"
          ),
          (
            "<http://example.org/3>",
            "<http://example.org/c>",
            "<http://example.org/4>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s ex:a{1}|ex:b{1,3}|ex:c{2,} ?o .
            | ?s ^!^ex:a*/ex:b?|ex:c+ ?o .
            | ?s ex:a ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }
    }
  }

}
