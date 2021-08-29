package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row
import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InclusiveGraphsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  val inclusiveConf = config.copy(isDefaultGraphExclusive = false)

  "execute queries" when {

    "inclusive" should {

      "query 1" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |SELECT ?s ?p ?o
            |WHERE { ?s ?p ?o }
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\""
          ),
          Row(
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\""
          )
        )
      }

      "query 2" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX dm: <http://dm.org/>
            |
            |SELECT ?s ?p ?o
            |FROM dm:graph1
            |FROM dm:graph2
            |WHERE { ?s ?p ?o }
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\""
          )
        )
      }

      "query 3" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX dm: <http://dm.org/>
            |
            |SELECT ?s ?p ?o
            |WHERE {
            | GRAPH dm:graph3 { ?s ?p ?o }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\""
          )
        )
      }

      "query 4" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX dm: <http://dm.org/>
            |
            |SELECT ?s ?p ?o
            |FROM dm:graph1
            |FROM dm:graph2
            |WHERE {
            | GRAPH dm:graph2 { ?s ?p ?o }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\""
          )
        )
      }

      "query 5" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX dm: <http://dm.org/>
            |
            |SELECT ?s ?p ?o
            |FROM dm:graph1
            |FROM NAMED dm:graph2
            |WHERE {
            | {?s ?p ?o}
            | UNION
            | {GRAPH dm:graph2 {?s ?p ?o } }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\""
          )
        )
      }

      "query 6" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX dm: <http://dm.org/>
            |
            |SELECT ?s ?p ?o
            |WHERE {
            | {GRAPH dm:graph1 {?s ?p ?o } }
            | UNION
            | {GRAPH dm:graph2 {?s ?p ?o } }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\""
          ),
          Row(
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\""
          )
        )
      }

      "query 7" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX dm: <http://dm.org/>
            |
            |SELECT ?s ?p ?o
            |FROM dm:graph1
            |FROM dm:graph3
            |WHERE {
            | GRAPH dm:graph1 {?s1 ?p ?o } .
            | GRAPH dm:graph3 {?s ?p ?s2 } .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\""
          )
        )
      }

      "query 7b" in {

        val df = List(
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\"",
            "<http://dm.org/graph1>"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Alice\"",
            "<http://dm.org/graph2>"
          ),
          (
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Martha\"",
            "<http://dm.org/graph3>"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX dm: <http://dm.org/>
            |
            |SELECT ?s ?p ?o
            |WHERE {
            | GRAPH dm:graph1 {?s1 ?p ?o } .
            | GRAPH dm:graph3 {?s ?p ?s2 } .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, inclusiveConf)

        result.right.get.collect().toSet shouldEqual Set(
          Row(
            "<http://example.org/martha>",
            "<http://xmlns.com/foaf/0.1/>",
            "\"Bob\""
          )
        )
      }
    }
  }

}
