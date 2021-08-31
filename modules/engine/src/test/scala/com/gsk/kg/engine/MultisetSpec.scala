package com.gsk.kg.engine

import higherkindness.droste.contrib.NewTypesSyntax.NewTypesOps
import higherkindness.droste.util.newtypes.@@

import org.apache.spark.sql.DataFrame

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.relational.Relational
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.engine.utils.MultisetMatchers
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

import org.scalatest.wordspec.AnyWordSpec

class MultisetSpec extends AnyWordSpec with SparkSpec with MultisetMatchers {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Multiset.join" should {

    "join two empty multisets together" in {
      val ms1 = Multiset.empty
      val ms2 = Multiset.empty

      ms1.join(ms2) should equalsMultiset(Multiset.empty)
    }

    "join other nonempty Multiset[A] on the right" in {
      import sqlContext.implicits._
      val empty = Multiset.empty[DataFrame @@ Untyped]
      val nonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph2"))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      empty.join(nonEmpty) should equalsMultiset(nonEmpty)
    }

    "join other nonempty Multiset[A] on the left" in {
      import sqlContext.implicits._
      val empty = Multiset.empty[DataFrame @@ Untyped]
      val nonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph2"))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      nonEmpty.join(empty) should equalsMultiset(nonEmpty)
    }

    "join other Multiset[A] when they have both the same single binding" in {
      import sqlContext.implicits._
      val variables = Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s))
      val df1 = List(
        ("test1", "graph1"),
        ("test2", "graph1"),
        ("test3", "graph1")
      ).toDF("d", GRAPH_VARIABLE.s).@@[Untyped]
      val df2 = List(
        ("test2", "graph1"),
        ("test3", "graph2"),
        ("test4", "graph2")
      ).toDF("d", GRAPH_VARIABLE.s).@@[Untyped]

      val ms1 = Multiset(variables, df1)
      val ms2 = Multiset(variables, df2)

      ms1.join(ms2) should equalsMultiset(
        Multiset(
          variables,
          List(("test2", "graph1"), ("test3", ""))
            .toDF("d", GRAPH_VARIABLE.s)
            .@@[Untyped]
        )
      )
    }

    "join other Multiset[A] when they share one binding" in {
      import sqlContext.implicits._
      val d = VARIABLE("d")
      val e = VARIABLE("e")
      val f = VARIABLE("f")

      val ms1 = Multiset(
        Set(d, e, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test1", "123", "graph1"),
          ("test2", "456", "graph1"),
          ("test3", "789", "graph1")
        )
          .toDF(d.s, e.s, GRAPH_VARIABLE.s)
          .@@[Untyped]
      )
      val ms2 = Multiset(
        Set(d, f, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "hello", "graph1"),
          ("test3", "goodbye", "graph2"),
          ("test4", "hola", "graph2")
        )
          .toDF(d.s, f.s, GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      ms1.join(ms2) should equalsMultiset(
        Multiset(
          Set(d, e, f, VARIABLE(GRAPH_VARIABLE.s)),
          List(
            ("test2", "456", "hello", "graph1"),
            ("test3", "789", "goodbye", "")
          )
            .toDF("d", "e", "f", GRAPH_VARIABLE.s)
            .@@[Untyped]
        )
      )
    }

    "join other Multiset[A] when they share more than one binding" in {
      import sqlContext.implicits._
      val d = VARIABLE("d")
      val e = VARIABLE("e")
      val f = VARIABLE("f")
      val g = VARIABLE("g")
      val h = VARIABLE("h")

      val ms1 = Multiset(
        Set(d, e, g, h, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test1", "123", "g1", "h1", "graph1"),
          ("test2", "456", "g2", "h2", "graph1"),
          ("test3", "789", "g3", "h3", "graph1")
        ).toDF(d.s, e.s, g.s, h.s, GRAPH_VARIABLE.s).@@[Untyped]
      )
      val ms2 = Multiset(
        Set(d, e, f, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "456", "hello", "graph1"),
          ("test3", "789", "goodbye", "graph2"),
          ("test4", "012", "hola", "graph2")
        ).toDF(d.s, e.s, f.s, GRAPH_VARIABLE.s).@@[Untyped]
      )

      val result = ms1.join(ms2)
      val expectedResult = Multiset(
        Set(d, e, f, g, h, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "456", "g2", "h2", "hello", "graph1"),
          ("test3", "789", "g3", "h3", "goodbye", "")
        ).toDF("d", "e", "g", "h", "f", GRAPH_VARIABLE.s).@@[Untyped]
      )

      result should equalsMultiset(expectedResult)
    }

    // TODO: This test if being ignore due to fix for: https://github.com/gsk-aiops/bellman/issues/357
    // It should be un-ignore when we address this ticket: https://github.com/gsk-aiops/bellman/issues/409
    "perform a cartesian product when there's no shared bindings between multisets" ignore {
      import sqlContext.implicits._
      val d = VARIABLE("d")
      val e = VARIABLE("e")
      val f = VARIABLE("f")
      val g = VARIABLE("g")
      val h = VARIABLE("h")
      val i = VARIABLE("i")

      val ms1 = Multiset(
        Set(d, e, f, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test1", "123", "g1", "graph1"),
          ("test2", "456", "g2", "graph1"),
          ("test3", "789", "g3", "graph1")
        ).toDF(d.s, e.s, f.s, GRAPH_VARIABLE.s).@@[Untyped]
      )
      val ms2 = Multiset(
        Set(g, h, i, VARIABLE(GRAPH_VARIABLE.s)),
        List(
          ("test2", "456", "hello", "graph1"),
          ("test3", "789", "goodbye", "graph2"),
          ("test4", "012", "hola", "graph2")
        ).toDF(g.s, h.s, i.s, GRAPH_VARIABLE.s).@@[Untyped]
      )

      val result = ms1.join(ms2)
      result should equalsMultiset(
        Multiset(
          Set(d, e, f, g, h, i, VARIABLE(GRAPH_VARIABLE.s)),
          List(
            ("test1", "123", "g1", "graph1", "test2", "456", "hello", "graph1"),
            (
              "test1",
              "123",
              "g1",
              "graph1",
              "test3",
              "789",
              "goodbye",
              "graph2"
            ),
            ("test1", "123", "g1", "graph1", "test4", "012", "hola", "graph2"),
            ("test2", "456", "g2", "graph1", "test2", "456", "hello", "graph1"),
            (
              "test2",
              "456",
              "g2",
              "graph1",
              "test3",
              "789",
              "goodbye",
              "graph2"
            ),
            ("test2", "456", "g2", "graph1", "test4", "012", "hola", "graph2"),
            ("test3", "789", "g3", "graph1", "test2", "456", "hello", "graph1"),
            (
              "test3",
              "789",
              "g3",
              "graph1",
              "test3",
              "789",
              "goodbye",
              "graph2"
            ),
            ("test3", "789", "g3", "graph1", "test4", "012", "hola", "graph2")
          ).toDF(
            "d",
            "e",
            "g",
            GRAPH_VARIABLE.s,
            "h",
            "f",
            "i",
            GRAPH_VARIABLE.s
          ).@@[Untyped]
        )
      )
    }
  }

  "Multiset.leftJoin" should {

    "return empty Multiset[A] when left joined two empty multisets" in {
      val ms1 = Multiset.empty
      val ms2 = Multiset.empty

      val result = ms1.leftJoin(ms2)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(Multiset.empty)
    }

    "return empty Multiset[A] when empty Multiset[A] on left and non empty Multiset[A] on right" in {
      import sqlContext.implicits._
      val leftEmpty = Multiset.empty
      val rightNonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph1"))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = leftEmpty.leftJoin(rightNonEmpty)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(leftEmpty)
    }

    "return right Multiset[A] when non empty Multiset[A] on left and empty Multiset[A] on right" in {
      import sqlContext.implicits._
      val rightEmpty = Multiset.empty
      val leftNonEmpty = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        Seq(("test1", "graph1"), ("test2", "graph1"))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = leftNonEmpty.leftJoin(rightEmpty)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(leftNonEmpty)
    }

    "return left Multiset[A] joined with right Multiset[A] when they have both the same single binding" in {
      import sqlContext.implicits._
      val variables = Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s))
      val leftDf = List(
        ("test1", "graph1"),
        ("test2", "graph1")
      ).toDF("d", GRAPH_VARIABLE.s).@@[Untyped]
      val rightDf = List(
        ("test1", "graph2"),
        ("test3", "graph2")
      ).toDF("d", GRAPH_VARIABLE.s).@@[Untyped]

      val left  = Multiset(variables, leftDf)
      val right = Multiset(variables, rightDf)

      val result = left.leftJoin(right)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          variables,
          List(("test1", "graph1"), ("test2", "graph1"))
            .toDF("d", GRAPH_VARIABLE.s)
            .@@[Untyped]
        )
      )
    }

    "return left Multiset[A] joined with right Multiset[A] when they share one binding" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, graph),
        List(("test1", "234", "graph1"), ("test2", "123", "graph1"))
          .toDF(d.s, e.s, graph.s)
          .@@[Untyped]
      )
      val right = Multiset(
        Set(d, f, graph),
        List(("test1", "hello", "graph2"), ("test3", "goodbye", "graph2"))
          .toDF(d.s, f.s, graph.s)
          .@@[Untyped]
      )

      val result = left.leftJoin(right)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(d, e, f, graph),
          List(
            ("test1", "234", "hello", "graph1"),
            ("test2", "123", null, "graph1")
          ).toDF("d", "e", "f", graph.s).@@[Untyped]
        )
      )
    }

    "return left when there's no shared bindings between multisets" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val g     = VARIABLE("g")
      val h     = VARIABLE("h")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", "graph1"),
          ("test2", "123", "g2", "h2", "graph2")
        )
          .toDF(d.s, e.s, g.s, h.s, graph.s)
          .@@[Untyped]
      )
      val right = Multiset(
        Set(d, e, f, graph),
        List(
          ("test1", "234", "hello", "graph3"),
          ("test3", "e2", "goodbye", "graph4")
        )
          .toDF(d.s, e.s, f.s, graph.s)
          .@@[Untyped]
      )

      val result = left.leftJoin(right)
      val expectedResult = Multiset(
        Set(d, e, f, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", "hello", "graph1"),
          ("test2", "123", "g2", "h2", null, "graph2")
        ).toDF("d", "e", "g", "h", "f", graph.s).@@[Untyped]
      )

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(expectedResult)
    }
  }

  "Multiset.union" should {

    "union two empty multisets together" in {
      val left  = Multiset.empty
      val right = Multiset.empty

      left.union(right) should equalsMultiset(Multiset.empty)
    }

    "union other non empty Multiset[A] on right" in {
      import sqlContext.implicits._

      val left = Multiset.empty
      val right = Multiset(
        Set(VARIABLE("a"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("A", "graph1"), ("B", "graph2"), ("C", "graph3"))
          .toDF("a", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      left.union(right) should equalsMultiset(right)
    }

    "union other non empty Multiset[A] on left" in {
      import sqlContext.implicits._

      val left = Multiset(
        Set(VARIABLE("a"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("A", "graph1"), ("B", "graph2"), ("C", "graph3"))
          .toDF("a", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )
      val right = Multiset.empty

      left.union(right) should equalsMultiset(left)
    }

    "union two multisets when they have both the single bindings" in {
      import sqlContext.implicits._
      val variables = Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s))

      val left = Multiset(
        variables,
        List(("test1", "graph1"), ("test2", "graph1"))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )
      val right = Multiset(
        variables,
        List(("test2", "graph2"), ("test3", "graph2"))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = left.union(right)
      val expectedResult = Multiset(
        variables,
        List(
          ("test1", "graph1"),
          ("test2", "graph1"),
          ("test2", "graph2"),
          ("test3", "graph2")
        ).toDF("d", GRAPH_VARIABLE.s).@@[Untyped]
      )

      result should equalsMultiset(expectedResult)
    }

    "union two multisets when they share one binding" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, graph),
        List(("test1", "234", "graph1"), ("test2", "123", "graph1"))
          .toDF(d.s, e.s, graph.s)
          .@@[Untyped]
      )
      val right = Multiset(
        Set(d, f),
        List(("test1", "hello", "graph2"), ("test3", "goodbye", "graph2"))
          .toDF(d.s, f.s, graph.s)
          .@@[Untyped]
      )

      left.union(right) should equalsMultiset(
        Multiset(
          Set(d, e, f, graph),
          List(
            ("test1", "234", null, "graph1"),
            ("test2", "123", null, "graph1"),
            ("test1", null, "hello", "graph2"),
            ("test3", null, "goodbye", "graph2")
          ).toDF("d", "e", "f", graph.s).@@[Untyped]
        )
      )
    }

    "union other Multiset[A] when they share more than one binding" in {
      import sqlContext.implicits._
      val d     = VARIABLE("d")
      val e     = VARIABLE("e")
      val f     = VARIABLE("f")
      val g     = VARIABLE("g")
      val h     = VARIABLE("h")
      val graph = VARIABLE(GRAPH_VARIABLE.s)

      val left = Multiset(
        Set(d, e, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", "graph1"),
          ("test2", "123", "g2", "h2", "graph1")
        )
          .toDF(d.s, e.s, g.s, h.s, graph.s)
          .@@[Untyped]
      )
      val right = Multiset(
        Set(d, e, f, graph),
        List(
          ("test1", "234", "hello", "graph2"),
          ("test3", "e2", "goodbye", "graph2")
        )
          .toDF(d.s, e.s, f.s, graph.s)
          .@@[Untyped]
      )

      val result = left.union(right)
      val expectedResult = Multiset(
        Set(d, e, f, g, h, graph),
        List(
          ("test1", "234", "g1", "h1", null, "graph1"),
          ("test2", "123", "g2", "h2", null, "graph1"),
          ("test1", "234", null, null, "hello", "graph2"),
          ("test3", "e2", null, null, "goodbye", "graph2")
        ).toDF("d", "e", "g", "h", "f", graph.s).@@[Untyped]
      )

      result should equalsMultiset(expectedResult)
    }
  }

  "Multiset.limit" should {

    "return solutions limited by l when l > 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.limit(2)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          List(("a", ""), ("b", "")).toDF("d", GRAPH_VARIABLE.s).@@[Untyped]
        )
      )
    }

    "return no solutions when l = 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.limit(0)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          Relational[DataFrame @@ Untyped].empty
        )
      )
    }

    "return error when l > MAX_INTEGER" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.limit(Integer.MAX_VALUE.toLong + 1)

      result shouldBe a[Left[_, _]]
      result.left.get shouldBe a[EngineError.NumericTypesDoNotMatch]
    }

    "return error when l < 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.limit(-1)

      result shouldBe a[Left[_, _]]
      result.left.get shouldBe a[EngineError.UnexpectedNegative]
    }
  }

  "Multiset.offset" should {

    "return solutions form offset" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.offset(1)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          List(("b", ""), ("c", "")).toDF("d", GRAPH_VARIABLE.s).@@[Untyped]
        )
      )
    }

    "return no solutions when offset is greater than size of initial dataframe" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.offset(5)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          Relational[DataFrame @@ Untyped].empty
        )
      )
    }

    "return all solutions when offset is 0" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.offset(0)

      result shouldBe a[Right[_, _]]
      result.right.get should equalsMultiset(
        Multiset(
          Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
          List(("a", ""), ("b", ""), ("c", ""))
            .toDF("d", GRAPH_VARIABLE.s)
            .@@[Untyped]
        )
      )
    }

    "return 0 when offset is negative" in {
      import sqlContext.implicits._
      val m = Multiset(
        Set(VARIABLE("d"), VARIABLE(GRAPH_VARIABLE.s)),
        List(("a", ""), ("b", ""), ("c", ""))
          .toDF("d", GRAPH_VARIABLE.s)
          .@@[Untyped]
      )

      val result = m.offset(-1)

      result shouldBe a[Left[_, _]]
      result.left.get shouldBe a[EngineError.UnexpectedNegative]
    }
  }

  // TODO: Add unit tests for filter
  "Multiset.filter" should {}

  // TODO: Add unit test for distinct
  "Multiset.distinct" should {}

  // TODO: Add unit test for withColumn
  "Multiset.withColumn" should {}

  // TODO: Add unit test for select
  "Multiset.select" should {}

  // TODO: Add unit test for isEmpty
  "Multiset.empty" should {}

}
