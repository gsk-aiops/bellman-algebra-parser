package com.gsk.kg.engine.functions

import cats.kernel.Monoid

import higherkindness.droste.util.newtypes.@@

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.functions.Literals.nullLiteral
import com.gsk.kg.engine.relational.Relational
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.engine.relational.Relational.ops._

import scala.annotation.tailrec
import scala.util.Try

object PathFrame {

  /** PathFrame is an alias for a dataframe which contains paths of different lengths. E.g:
    *
    * For this DataFrame which contains SPO triples (path of 1 length):
    * +----------------------------+---------------------------------+----------------------------+
    * |s                           |p                                |o                           |
    * +----------------------------+---------------------------------+----------------------------+
    * |<http://example.org/Alice>  |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Bob>    |
    * |<http://example.org/Bob>    |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Charles>|
    * |<http://example.org/Charles>|<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Daniel> |
    * |<http://example.org/Daniel> |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Erick>  |
    * +----------------------------+---------------------------------+----------------------------+
    *
    * The counterpart PathFrame would be:
    * +----------------------------+---------------------------------+----------------------------+
    * |1                           |2                                |3                           |
    * +----------------------------+---------------------------------+----------------------------+
    * |<http://example.org/Alice>  |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Bob>    |
    * |<http://example.org/Bob>    |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Charles>|
    * |<http://example.org/Charles>|<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Daniel> |
    * |<http://example.org/Daniel> |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Erick>  |
    * +----------------------------+---------------------------------+----------------------------+
    * +---------------------------------+----------------------------+---------------------------------+
    * |4                                |5                           |6                                |
    * +---------------------------------+----------------------------+---------------------------------+
    * |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Charles>|<http://xmlns.org/foaf/0.1/knows>|
    * |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Daniel> |<http://xmlns.org/foaf/0.1/knows>|
    * |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Erick>  |null                             |
    * |null                             |null                        |null                             |
    * +---------------------------------+----------------------------+---------------------------------+
    * +---------------------------+---------------------------------+---------------------------+
    * |7                          |8                                |9                          |
    * +---------------------------+---------------------------------+---------------------------+
    * |<http://example.org/Daniel>|<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Erick> |
    * |<http://example.org/Erick> |<http://xmlns.org/foaf/0.1/knows>|null                       |
    * |null                       |null                             |null                       |
    * |null                       |null                             |null                       |
    * +---------------------------+---------------------------------+---------------------------+
    *
    * Where the graph that forms the initial DataFrame @@ Untyped has been traversed and generated a new DataFrame @@ Untyped that contains
    * the paths of the graph. Taking a look at this PathFrame we can see that it contains next path lengths:
    * - 1 length: triples from columns 1 to 3.
    * - 2 length: triples from columns 1 to 5.
    * - 3 length: triples from columns 1 to 7.
    */

  type PathFrame = DataFrame @@ Untyped

  val SIdx = 1
  val PIdx = 2
  val OIdx = 3

  /** It receives a base dataframe with all triples of the graph and creates a new dataframe which will contain all
    * existing paths. This is done by iterating and accumulating results for every new depth level until there are no
    * more triples to connect or it reaches some limit.
    * @param initial Base dataframe with all the one length paths
    * @param limit If some it will set a limit to iterate while constructing the PathFrame
    * @return Dataframe with all the triples connected creating paths
    */
  def constructPathFrame(
      initial: DataFrame @@ Untyped,
      limit: Option[Int]
  ): (Int, PathFrame) = {

    @tailrec
    def step(
        oneLengthPaths: PathFrame,
        accPaths: PathFrame,
        i: Int
    ): (Int, PathFrame) = {

      lazy val stepSlice: Int => Int = x => x + (2 * i)

      val stepColNames = (SIdx to stepSlice(OIdx)).map(_.toString)

      val stepPath = oneLengthPaths
        .withColumnRenamed(s"$OIdx", s"${stepSlice(OIdx)}")
        .withColumnRenamed(s"$PIdx", s"${stepSlice(PIdx)}")
        .withColumnRenamed(s"$SIdx", s"${stepSlice(SIdx)}")

      val stepAcc = accPaths
        .leftOuter(
          stepPath,
          Seq(s"${stepSlice(SIdx)}")
        )
        .select(stepColNames.map(col))

      val continueTraversing = !stepAcc
        .filter(stepAcc.getColumn(s"${stepSlice(OIdx)}").isNotNull)
        .isEmpty

      limit match {
        case Some(l) if continueTraversing && i < l =>
          step(oneLengthPaths, stepAcc, i + 1)
        case _ if continueTraversing =>
          step(oneLengthPaths, stepAcc, i + 1)
        case _ =>
          (i, accPaths)
      }
    }

    val i = 1
    step(initial, initial, i)
  }

  /** This function will return a PathFrame containing the path 0 length for a given dataframe.
    * @param df
    * @return
    */
  def getOneLengthPaths(df: DataFrame @@ Untyped): PathFrame = {
    df
      .drop("g")
      .withColumnRenamed("s", s"$SIdx")
      .withColumnRenamed("p", s"$PIdx")
      .withColumnRenamed("o", s"$OIdx")
  }

  /** This function a PathFrame with all the vertex that points to itself (0 length path),
    * the predicate column will contain null values.
    * @param df
    * @return
    */
  def getZeroLengthPaths(df: DataFrame @@ Untyped): PathFrame = {
    val sDf = df.select(Seq(col("s")))
    val oDf = df.select(Seq(col("o")))

    val vertices = sDf.union(oDf).distinct
    vertices
      .withColumn("p", nullLiteral)
      .withColumn("o", vertices.getColumn("s"))
      .withColumnRenamed("s", s"$SIdx")
      .withColumnRenamed("p", s"$PIdx")
      .withColumnRenamed("o", s"$OIdx")
  }

  /** This function will return the n paths length triples from a given PathFrame.
    * @param df Initial Dataframe (this is needed because for path 0 length we need to know all vertex, also the ones
    *           not included in the PathFrame).
    * @param pathFrame The PathFrame that contains all length paths.
    * @param nPath The n length path that is wanted to be extracted.
    * @return
    */
  def getNLengthPathTriples(
      df: DataFrame @@ Untyped,
      pathFrame: PathFrame,
      nPath: Int
  ): PathFrame = {
    if (nPath == 0) {
      getZeroLengthPaths(df)
    } else {

      val oSlice = OIdx + (2 * (nPath - 1))

      val cols = Seq(SIdx.toString, PIdx.toString, oSlice.toString)

      val nPaths = pathFrame
        .select(cols.map(col))
        .filter(cols.foldLeft(lit(true)) { case (acc, elem) =>
          acc && pathFrame.getColumn(elem).isNotNull
        })
        .withColumnRenamed(s"$oSlice", s"$OIdx")

      nPaths
    }
  }

  /** It merges two dataframes by adding rows from the right dataframe to the second and setting to null those columns
    * that are not present in the right one.
    * @param df1
    * @param df2
    * @return
    */
  def merge(
      df1: DataFrame @@ Untyped,
      df2: DataFrame @@ Untyped
  ): DataFrame @@ Untyped = {

    def getNewColumns(column: Set[String], mergedCols: Set[String]) = {
      mergedCols.toList.map {
        case x if column.contains(x) => col(x)
        case x @ _                   => nullLiteral.as(x)
      }
    }

    val mergedCols = df1.columns.toSet ++ df2.columns.toSet
    val newDf1 =
      df1.select(getNewColumns(df1.columns.toSet, mergedCols))
    val newDf2 =
      df2.select(getNewColumns(df2.columns.toSet, mergedCols))

    newDf1.unionByName(newDf2)
  }

  /** It transforms a PathFrame into a Dataframe renaming columns to SPOG.
    * @param pf Pathframe that is wanted to be converted.
    * @return
    */
  def toSPOG(pf: PathFrame): DataFrame @@ Untyped = {

    def hasColumn(df: DataFrame @@ Untyped, path: String) = Try(path).isSuccess

    val df = Seq((SIdx, "s"), (PIdx, "p"), (OIdx, "o")).foldLeft(pf) {
      case (accDf, (idx, name)) =>
        if (hasColumn(accDf, idx.toString)) {
          accDf.withColumnRenamed(idx.toString, name)
        } else {
          accDf.withColumn(name, nullLiteral)
        }
    }

    // TODO: The graph column should be propagated across the PathFrame instead of added to default graph
    // See: GSK issue AIPL-3665
    if (hasColumn(df, "g")) {
      df
    } else {
      df.withColumn("g", lit(""))
    }
  }

  implicit def monoid(implicit
      sc: SQLContext
  ): Monoid[PathFrame] =
    new Monoid[PathFrame] {
      def combine(x: PathFrame, y: PathFrame): PathFrame =
        merge(x, y).distinct
      def empty: PathFrame = Relational[PathFrame].empty
    }
}
