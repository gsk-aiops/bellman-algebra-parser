package com.gsk.kg.engine.functions

import cats.kernel.Monoid

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

import com.gsk.kg.engine.functions.Literals.nullLiteral

import scala.annotation.tailrec

object PathFrame {

  // scalastyle: off
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
    * +---------------------------------+----------------------------+---------------------------------+---------------------------+
    * |4                                |5                           |6                                |7                          |
    * +---------------------------------+----------------------------+---------------------------------+---------------------------+
    * |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Charles>|<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Daniel>|
    * |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Daniel> |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Erick> |
    * |<http://xmlns.org/foaf/0.1/knows>|<http://example.org/Erick>  |null                             |null                       |
    * |null                             |null                        |null                             |null                       |
    * +---------------------------------+----------------------------+---------------------------------+---------------------------+
    *
    * Where the graph that forms the initial DataFrame has been traversed and generated a new DataFrame that contains
    * the paths of the graph. Taking a look at this PathFrame we can see that it contains next path lengths:
    * - 1 length: triples from columns 1 to 3.
    * - 2 length: triples from columns 1 to 5.
    * - 3 length: triples from columns 1 to 7.
    */
  // scalastyle: on
  type PathFrame = DataFrame

  val SIdx = 1
  val PIdx = 2
  val OIdx = 3

  /** It receives a base dataframe with all triples of the graph and creates a new dataframe which will contain all
    * existing paths. This is done by iterating and accumulating results for every new depth level until there are no
    * more triples to connect.
    * @param baseDf Base dataframe with all the triples unconnected
    * @param accDf The dataframe which it will accumulate every depth level of paths
    * @param i Iteration index
    * @param sIdx Subject index (for column renaming)
    * @param pIdx Predicate index (for column renaming)
    * @param oIdx Object index (for column renaming
    * @return Dataframe with all the triples connected creating paths
    */
  def constructPathFrame(
      oneLengthPaths: DataFrame,
      accPaths: DataFrame,
      limit: Option[Int]
  ): (Int, PathFrame) = {

    @tailrec
    def step(
        oneLengthPaths: DataFrame,
        accPaths: DataFrame,
        i: Int
    ): (Int, DataFrame) = {

      lazy val stepSlice: Int => Int = x => x + (2 * i)

      val stepColNames = (SIdx to stepSlice(OIdx)).map(_.toString)

      val stepPath = oneLengthPaths
        .withColumnRenamed(s"$OIdx", s"${stepSlice(OIdx)}")
        .withColumnRenamed(s"$PIdx", s"${stepSlice(PIdx)}")
        .withColumnRenamed(s"$SIdx", s"${stepSlice(SIdx)}")

      val stepAcc = accPaths
        .join(
          stepPath,
          Seq(s"${stepSlice(SIdx)}"),
          "left_outer"
        )
        .select(stepColNames.head, stepColNames.tail: _*)

      val continueTraversing = !stepAcc
        .filter(stepAcc(s"${stepSlice(OIdx)}").isNotNull)
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
    step(oneLengthPaths, accPaths, i)
  }

  def getOneLengthPaths(df: DataFrame): DataFrame = {
    df
      .drop("g")
      .withColumnRenamed("s", s"$SIdx")
      .withColumnRenamed("p", s"$PIdx")
      .withColumnRenamed("o", s"$OIdx")
  }

  /** It returns a dataframe with all the vertex that points to itselft (0 length path), the predicate column will
    * contain null values.
    * @param df
    * @return
    */
  def getZeroLengthPaths(df: DataFrame): DataFrame = {
    val sDf = df.select(df("s"))
    val oDf = df.select(df("o"))

    val vertices = sDf.union(oDf).distinct()
    vertices
      .withColumn("p", nullLiteral)
      .withColumn("o", vertices("s"))
      .withColumnRenamed("s", s"$SIdx")
      .withColumnRenamed("p", s"$PIdx")
      .withColumnRenamed("o", s"$OIdx")
  }

  /** @param pathDf
    * @param nPath
    * @return
    */
  def getNLengthPathTriples(
      df: DataFrame,
      pathDf: DataFrame,
      nPath: Int
  ): DataFrame = {
    if (nPath == 0) {
      getZeroLengthPaths(df)
    } else {

      val oSlice = OIdx + (2 * (nPath - 1))

      val cols = Seq(SIdx.toString, PIdx.toString, oSlice.toString)

      val nPaths = pathDf
        .select(cols.head, cols.tail: _*)
        .filter(cols.foldLeft(lit(true)) { case (acc, elem) =>
          acc && pathDf(elem).isNotNull
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
  def merge(df1: DataFrame, df2: DataFrame): DataFrame = {

    def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
      merged_cols.toList.map {
        case x if column.contains(x) => col(x)
        case x @ _                   => nullLiteral.as(x)
      }
    }

    val merged_cols = df1.columns.toSet ++ df2.columns.toSet
    val newDf1 =
      df1.select(getNewColumns(df1.columns.toSet, merged_cols): _*)
    val newDf2 =
      df2.select(getNewColumns(df2.columns.toSet, merged_cols): _*)

    newDf1.unionByName(newDf2)
  }

  def toSPO(pf: PathFrame): DataFrame = {
    pf
      .withColumnRenamed(s"$SIdx", "s")
      .withColumnRenamed(s"$PIdx", "p")
      .withColumnRenamed(s"$OIdx", "o")
  }

  implicit def monoid(implicit
      sc: SQLContext
  ): Monoid[PathFrame] =
    new Monoid[PathFrame] {
      def combine(x: PathFrame, y: PathFrame): PathFrame =
        merge(x, y).distinct()
      def empty: PathFrame = sc.emptyDataFrame
    }
}
