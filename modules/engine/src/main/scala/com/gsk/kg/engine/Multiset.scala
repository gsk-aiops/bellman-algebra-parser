package com.gsk.kg.engine

import cats.kernel.Monoid
import cats.syntax.all._

import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.relational.Relational
import com.gsk.kg.engine.relational.Relational.ops._
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Result
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

/** A [[Multiset[A]]], as expressed in SparQL terms.
  *
  * @see See [[https://www.w3.org/2001/sw/DataAccess/rq23/rq24-algebra.html]]
  * @param bindings the variables this Multiset[A] expose
  * @param relational the underlying data that the Multiset[A] contains
  */
final case class Multiset[A: Relational](
    bindings: Set[VARIABLE],
    relational: A
) {

  /** Join two multisets following SparQL semantics.  If two multisets
    * share some bindings, it performs an _inner join_ between them.
    * If they don't share any common binding, it performs a cross join
    * instead.
    *
    * =Spec=
    *
    * Defn: Join
    * Let Ω1 and Ω2 be multisets of mappings. We define:
    * Join(Ω1, Ω2) = { merge(μ1, μ2) | μ1 in Ω1and μ2 in Ω2, and μ1 and μ2 are compatible }
    * card[Join(Ω1, Ω2)](μ) = sum over μ in (Ω1 set-union Ω2), card[Ω1](μ1)*card[Ω2](μ2)
    *
    * @param other
    * @return the join result of both multisets
    */
  def join(other: Multiset[A])(implicit sc: SQLContext): Multiset[A] = {
    (this, other) match {
      case (l, r) if l.isEmpty => r
      case (l, r) if r.isEmpty => l
      case (l, r) if noCommonBindings(l, r) =>
        Multiset(
          l.bindings union r.bindings,
          l.relational.crossJoin(r.relational.drop(GRAPH_VARIABLE.s))
        )
      case (l, r) =>
        Multiset(
          l.bindings union r.bindings,
          innerJoinWithGraphsColumn(l.relational, r.relational)
        )
    }
  }

  /** A left join returns all values from the left relation and the matched values from the right relation,
    * or appends NULL if there is no match. It is also referred to as a left outer join.
    * @param other
    * @return
    */
  def leftJoin(other: Multiset[A]): Result[Multiset[A]] = ((this, other) match {
    case (l, r) if l.isEmpty => l
    case (l, r) if r.isEmpty => l
    case (l, r) if noCommonBindings(l, r) =>
      val df = l.relational.crossJoin(r.relational)
      Multiset(l.bindings union r.bindings, df)
    case (l, r) =>
      val df =
        l.relational.leftJoin(
          filterGraph(r).relational,
          (l.bindings intersect filterGraph(r).bindings).toSeq
            .map(_.s)
        )
      Multiset(l.bindings union r.bindings, df)
  }).asRight

  /** Perform a union between [[this]] and [[other]], as described in
    * SparQL Algebra doc.
    *
    * =Spec=
    *
    * Defn: Union
    * Let Ω1 and Ω2 be multisets of mappings. We define:
    * Union(Ω1, Ω2) = { μ | μ in Ω1 or μ in Ω2 }
    * card[Union(Ω1, Ω2)](μ) = card[Ω1](μ) + card[Ω2](μ)
    *
    * @param other
    * @return the Union of both multisets
    */
  def union(other: Multiset[A]): Multiset[A] = (this, other) match {
    case (a, b) if a.isEmpty => b
    case (a, b) if b.isEmpty => a
    case (Multiset(aBindings, aDF), Multiset(bBindings, bDF))
        if aDF.columns == bDF.columns =>
      Multiset(
        aBindings.union(bBindings),
        aDF.union(bDF)
      )
    case (Multiset(aBindings, aDF), Multiset(bBindings, bDF)) =>
      val colsA     = aDF.columns.toSet
      val colsB     = bDF.columns.toSet
      val colsUnion = colsA.union(colsB)

      def genColumns(current: Set[String], total: Set[String]): Seq[Column] = {
        total.toList.sorted.map {
          case x if current.contains(x) => col(x)
          case x                        => lit(null).as(x) // scalastyle:ignore
        }
      }

      val bindingsUnion = aBindings union bBindings
      val selectionA    = aDF.select(genColumns(colsA, colsUnion))
      val selectionB    = bDF.select(genColumns(colsB, colsUnion))

      Multiset(
        bindingsUnion,
        selectionA.unionByName(selectionB)
      )
  }

  def minus(other: Multiset[A]): Multiset[A] = (this, other) match {
    case (a, b) if a.isEmpty => b
    case (a, b) if b.isEmpty => a
    case (a, b) =>
      Multiset(
        a.bindings.union(b.bindings),
        a.relational.minus(b.relational)
      )
  }

  /** Return wether both the dataframe & bindings are empty
    *
    * @return
    */
  def isEmpty: Boolean = bindings.isEmpty && relational.columns.isEmpty

  /** Get a new Multiset[A] with only the projected [[vars]].
    *
    * @param vars
    * @return
    */
  def select(vars: VARIABLE*): Multiset[A] =
    Multiset(
      bindings.intersect(vars.toSet),
      relational.select(vars.map(v => col(v.s)))
    )

  /** Add a new column to the Multiset[A], with the given binding
    *
    * @param binding
    * @param col
    * @param fn
    * @return
    */
  def withColumn(
      binding: VARIABLE,
      column: Column
  ): Multiset[A] =
    Multiset(
      bindings + binding,
      relational
        .withColumn(binding.s, column)
    )

  /** A limited solutions sequence has at most given, fixed number of members.
    * The limit solution sequence S = (S0, S1, ..., Sn) is
    * limit(S, m) =
    * (S0, S1, ..., Sm-1) if n > m
    * (S0, S1, ..., Sn) if n <= m-1
    * @param limit
    * @return
    */
  def limit(limit: Long): Result[Multiset[A]] = limit match {
    case l if l < 0 =>
      EngineError.UnexpectedNegative("Negative limit: $l").asLeft
    case l if l > Int.MaxValue.toLong =>
      EngineError
        .NumericTypesDoNotMatch(s"$l to big to be converted to an Int")
        .asLeft
    case l =>
      this.copy(relational = relational.limit(l.toInt)).asRight
  }

  /** An offset solution sequence with respect to another solution sequence S, is one which starts at a given index of S.
    * For solution sequence S = (S0, S1, ..., Sn), the offset solution sequence
    * offset(S, k), k >= 0 is
    * (Sk, Sk+1, ..., Sn) if n >= k
    * (), the empty sequence, if k > n
    * @param offset
    * @return
    */
  def offset(offset: Long)(implicit sc: SQLContext): Result[Multiset[A]] = if (
    offset < 0
  ) {
    EngineError.UnexpectedNegative(s"Negative offset: $offset").asLeft
  } else {
    this.copy(relational = relational.offset(offset)).asRight
  }

  /** Filter restrict the set of solutions according to a given expression.
    * @param col
    * @return
    */
  def filter(col: Column): Result[Multiset[A]] = {
    val filtered = relational.filter(col)
    this.copy(relational = filtered).asRight
  }

  /** Eliminates duplicates from the dataframe that matches the same variable binding
    * @return
    */
  def distinct: Result[Multiset[A]] = {
    this
      .copy(
        relational = this.relational.distinct
      )
      .asRight
  }

  /** Removes graph bindings and graph columns from Multiset
    */
  private val filterGraph: Multiset[A] => Multiset[A] = { m =>
    if (m.isEmpty) {
      m
    } else {
      m.copy(
        bindings = m.bindings.filter(_.s != GRAPH_VARIABLE.s),
        relational = m.relational.drop(GRAPH_VARIABLE.s)
      )
    }
  }

  /** Adds graph binding and column to the Multiset[A] (with default graph as the default value)
    */
  private val addDefaultGraph: Multiset[A] => Multiset[A] = { m =>
    if (m.isEmpty) {
      m
    } else {
      m.copy(
        bindings = m.bindings + VARIABLE(GRAPH_VARIABLE.s),
        relational = m.relational.withColumn(GRAPH_VARIABLE.s, lit(""))
      )
    }
  }

  /** Checks whether two Multisets has no common bindings without having into account graph bindings
    * @param l
    * @param r
    * @return
    */
  private def noCommonBindings(l: Multiset[A], r: Multiset[A]): Boolean =
    (filterGraph(l).bindings intersect filterGraph(r).bindings).isEmpty

  /** Checks whether two Multisets both contains a graph variable column
    * @param l
    * @param r
    * @return
    */
  private def containsGraphVariables(l: Multiset[A], r: Multiset[A]): Boolean =
    l.relational.columns.contains(GRAPH_VARIABLE.s) &&
      r.relational.columns.contains(GRAPH_VARIABLE.s)

  /** This methods is a utility to perform operations like [[union]], [[join]], [[leftJoin]] on Multisets without
    * taking into account graph bindings and graph columns on dataframes by:
    * removing from bindings and dataframe -> operate -> adding binding and column to final dataframe as default graph
    * @param left
    * @param right
    * @param f
    * @return
    */
  private def graphAgnostic(left: Multiset[A], right: Multiset[A])(
      f: (Multiset[A], Multiset[A]) => Multiset[A]
  ): Multiset[A] =
    addDefaultGraph(f(filterGraph(left), filterGraph(right)))

  /** This method performs a inner join between graphs by merging the graph columns of each dataframes into an array
    * column, if they have same graph then the graph is preserved, if not it is added as if in the default graph. Eg:
    * Initial dataframes:
    * l.dataframe = List(
    *   ("a", "graph1"),
    *   ("b", "graph1"),
    *   ("c", "graph1")
    * ).toDF("?x", "*g")
    * r.dataframe = List(
    *   ("b", "graph1"),
    *   ("c", "graph2"),
    *   ("d", "graph2")
    * ).toDF("?x", "*g")
    *
    * Step1:
    * step1Dataframe = List(
    *   ("b", "graph1", "graph1"),
    *   ("c", "graph1", "graph2")
    * ).toDF("?x", "*l", "*r")
    *
    * Step2:
    * step2Dataframe = List(
    *   ("b", List("graph1", "graph1")),
    *   ("c", List("graph1", "graph2"))
    * ).toDF("?x", "*g")
    *
    * Final:
    * final = List(
    *  ("b", "graph1"),
    *  ("c", ""),
    * ).toDF("?x", "*g")
    *
    * IMPORTANT!: This method could have some performance issues as it traverses entire dataframe, and should be revisited
    * for performance improvements.
    * @param l
    * @param r
    * @return
    */
  def innerJoinWithGraphsColumn(l: A, r: A)(implicit
      sc: SQLContext
  ): A = {
    val leftGraphCol  = "*l"
    val rightGraphCol = "*r"

    val renamedLeft =
      //l.as("l").withColumnRenamed(GRAPH_VARIABLE.s, leftGraphCol)
      l.withColumnRenamed(GRAPH_VARIABLE.s, leftGraphCol)
    val renamedRight =
      //r.as("r").withColumnRenamed(GRAPH_VARIABLE.s, rightGraphCol)
      r.withColumnRenamed(GRAPH_VARIABLE.s, rightGraphCol)

    val intersect = renamedLeft.columns intersect renamedRight.columns
    val innerJoin = renamedLeft.innerJoin(renamedRight, intersect)

    // Merges the *l and *r columns into a *g column with Array(l, r)
    val innerWithMergedGraphColumns = innerJoin
      .withColumn(GRAPH_VARIABLE.s, array(leftGraphCol, rightGraphCol))
      .drop(leftGraphCol)
      .drop(rightGraphCol)

    // Generates a schema for the final DF (needed for the flatMap)
    val resultSchema = innerWithMergedGraphColumns
      .withColumn(GRAPH_VARIABLE.s, lit(""))
      .schema

    // For each element on the array of *g column if all the graphs are the same we assign the graph
    // if not we assign default graph
    val result = innerWithMergedGraphColumns.map { r =>
      val index  = r.fieldIndex(GRAPH_VARIABLE.s)
      val graphs = r.getSeq[String](index)
      if (graphs.forall(_ == graphs.head)) {
        Row.fromSeq(r.toSeq.dropRight(1) :+ graphs.head)
      } else {
        Row.fromSeq(r.toSeq.dropRight(1) :+ "")
      }
    }(RowEncoder(resultSchema))

    // Needed to keep Schema information on each Row. We will have GenericRowWithSchema instead of GenericRow
    Relational[A]
      .fromDataFrame(
        sc.sparkSession
          .createDataFrame(result.toDataFrame.toJavaRDD, resultSchema)
      )
  }

}

object Multiset {

  def empty[A: Relational](implicit sc: SQLContext): Multiset[A] =
    Multiset(Set.empty, Relational[A].empty)

  def fromRelational[A: Relational](relational: A)(implicit
      sc: SQLContext
  ): Multiset[A] =
    Multiset(relational.columns.map(VARIABLE).toSet, relational)

  implicit def monoid[A: Relational](implicit
      sc: SQLContext
  ): Monoid[Multiset[A]] =
    new Monoid[Multiset[A]] {
      def combine(x: Multiset[A], y: Multiset[A]): Multiset[A] = x.join(y)
      def empty: Multiset[A]                                   = Multiset(Set.empty, Relational[A].empty)
    }
}
