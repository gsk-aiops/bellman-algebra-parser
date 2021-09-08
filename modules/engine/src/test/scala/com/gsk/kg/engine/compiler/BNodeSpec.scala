package com.gsk.kg.engine.compiler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.functions.count

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BNodeSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  lazy val df: DataFrame = List(
    ("_:a", "<http://xmlns.com/foaf/0.1/name>", "Alice", ""),
    ("_:b", "<http://xmlns.com/foaf/0.1/name>", "Bob", ""),
    ("_:c", "<http://xmlns.com/foaf/0.1/name>", "Alice", "")
  ).toDF("s", "p", "o", "g")

  val uuidRegex =
    "_:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}"

  val expected = (1 to 3).map(_ => Row(true)).toList

  "perform BNODE function correctly" when {

    "str is simple literal" in {
      val str         = "abc"
      val actual      = act(str)
      val strTyped    = "\"abc\"^^xsd:string"
      val actualTyped = act(strTyped)
      actual shouldEqual actualTyped
    }
  }

  private def act(str: String) = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/title>",
        str
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:titleHashed ?titleHashed .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(bnode(?title) as ?titleHashed) .
          }
          """

    Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .head()
  }

  "perform query with BNODE function" should {

    "SELECT bnode without name create a Blank node with UUID format" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT BNODE()
          |WHERE  {
          |   ?x foaf:name ?name .
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).rlike(uuidRegex)),
        query,
        expected
      )
    }

    "BIND bnode without name create a Blank node with UUID format" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(BNODE() as ?id)
          |
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).rlike(uuidRegex)),
        query,
        expected
      )
    }

    "The rows of a bnode without name are different" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT BNODE()
          |WHERE  {
          |   ?x foaf:name ?name .
          |}
          |""".stripMargin

      val wind     = Window.partitionBy(Evaluation.renamedColumn)
      val countRow = count(col(Evaluation.renamedColumn)).over(wind)
      val countDistinctRow = org.apache.spark.sql.functions
        .size(collect_set(Evaluation.renamedColumn).over(wind))

      Evaluation.eval(
        df,
        Some(countRow.equalTo(countDistinctRow)),
        query,
        expected
      )
    }

    "The names of a two bnodes without name are differents" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id ?id2
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(BNODE() as ?id) .
          |   bind(BNODE() as ?id2)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).notEqual(col("?id2"))),
        query,
        expected
      )
    }

    "The bnode created by with a specific label(input parameter) simple literal name,have a UUID format" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT BNODE("abc")
          |WHERE  {
          |   ?x foaf:name ?name .
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).rlike(uuidRegex)),
        query,
        expected
      )
    }

    "The bnode created by with a specific label(input parameter) simple literal name,have differents" +
      "values of rows" in {
        val query =
          """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT BNODE("abc")
          |WHERE  {
          |   ?x foaf:name ?name .
          |}
          |""".stripMargin

        val wind     = Window.partitionBy(Evaluation.renamedColumn)
        val countRow = count(col(Evaluation.renamedColumn)).over(wind)
        val countDistinctRow = org.apache.spark.sql.functions
          .size(collect_set(Evaluation.renamedColumn).over(wind))

        Evaluation.eval(
          df,
          Some(countRow.equalTo(countDistinctRow)),
          query,
          expected
        )
      }

    "The bnode created by with a variable,have a UUID format" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(BNODE(?name) as ?id) .
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).rlike(uuidRegex)),
        query,
        expected
      )
    }

    "The bnode created by with a variable name,have different values of rows" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT BNODE(?name)
          |WHERE  {
          |   ?x foaf:name ?name .
          |}
          |""".stripMargin

      val wind     = Window.partitionBy(Evaluation.renamedColumn)
      val countRow = count(col(Evaluation.renamedColumn)).over(wind)
      val countDistinctRow = org.apache.spark.sql.functions
        .size(collect_set(Evaluation.renamedColumn).over(wind))

      Evaluation.eval(
        df,
        Some(countRow.equalTo(countDistinctRow)),
        query,
        expected
      )
    }

    "bnodes created with a different specific simple literal label are different" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id ?id2
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(BNODE("tomy") as ?id) .
          |   bind(BNODE("pepe") as ?id2)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).notEqual(col("?id2"))),
        query,
        expected
      )
    }

    "bnodes created with the same specific simple literal label are equal" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id ?id2
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(BNODE("tomy") as ?id) .
          |   bind(BNODE("tomy") as ?id2)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).equalTo(col("?id2"))),
        query,
        expected
      )
    }

    "bnodes created with a different variable are different" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id ?id2
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(BNODE(?x) as ?id) .
          |   bind(BNODE(?name) as ?id2)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).notEqual(col("?id2"))),
        query,
        expected
      )
    }

    "bnodes created with the same variable are equal" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?id ?id2
          |WHERE  {
          |   ?x foaf:name ?name .
          |   bind(BNODE(?x) as ?id) .
          |   bind(BNODE(?x) as ?id2)
          |}
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(col(Evaluation.renamedColumn).equalTo(col("?id2"))),
        query,
        expected
      )
    }

    "bnodes in a subquery generates same and diferentes values" in {
      val query =
        """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |SELECT ?name ?id1 ?id2 ?id3
          |WHERE  {
          |   {
          |   SELECT ?id1 ?id2 ?name
          |   WHERE {
          |     ?x foaf:name ?name .
          |     bind(BNODE("a") as ?id1) .
          |     bind(BNODE("b") as ?id2) .
          |         }
          |       }
          |   bind(BNODE("b") as ?id3) .
          |   }
          |""".stripMargin

      Evaluation.eval(
        df,
        Some(
          col(Evaluation.renamedColumn).notEqual(col("?id2")) &&
            col("?id2").equalTo(col("?id3"))
        ),
        query,
        expected
      )
    }
  }
}
