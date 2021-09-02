package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

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

  "perform query with BNODE function" should {

    "Select Bnode with a generic name" in {
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

    "Bind BNODE with a generic name be differents" in {
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

    "Generate 2 generic BNODE with diferents name" in {
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
  }
}
