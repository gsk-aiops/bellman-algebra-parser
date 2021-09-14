package com.gsk.kg.engine.relational

import higherkindness.droste.contrib.NewTypesSyntax.NewTypesOps
import higherkindness.droste.util.newtypes.@@

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import com.gsk.kg.engine.relational.Relational.Untyped

import simulacrum.typeclass

/** The [[Relational]] typeclass captures the idea of a datatype [[A]] with
  * relational operations needed for compiling SparQL algebra
  *
  * =Laws=
  *
  * {{{
  * Relational[A].union(Relational[A].empty, Relational[A].empty) <=> Relational[A].empty
  * Relational[A].union(df, Relational[A].empty) <=> df
  * Relational[A].union(Relational[A].empty, df) <=> df
  * }}}
  */
@typeclass trait Relational[A] {
  def empty(implicit sc: SQLContext): A
  def isEmpty(df: A): Boolean
  def crossJoin(left: A, right: A): A
  def innerJoin(left: A, right: A, columns: Seq[String]): A
  def leftJoin(left: A, right: A, columns: Seq[String]): A
  def innerJoin(left: A, right: A, expression: Column): A
  def leftJoin(left: A, right: A, expression: Column): A
  def leftSemi(left: A, right: A, columns: Seq[String]): A
  def leftAnti(left: A, right: A, columns: Seq[String]): A
  def leftOuter(df: A, right: A, columns: Seq[String]): A
  def union(left: A, right: A): A
  def unionByName(left: A, right: A): A
  def minus(left: A, right: A): A
  def offset(df: A, offset: Long)(implicit sc: SQLContext): A
  def limit(df: A, offset: Long): A
  def filter(df: A, condition: Column): A
  def distinct(df: A): A
  def drop(df: A, column: String): A
  def withColumn(df: A, name: String, expression: Column): A
  def withColumnRenamed(df: A, oldName: String, newName: String): A
  def columns(df: A): Array[String]
  def select(df: A, columns: Seq[Column]): A
  def select(df: A, column: Column): A =
    select(df, Seq(column))
  def schema(df: A): StructType
  def fromDataFrame(df: DataFrame): A
  def getColumn(df: A, col: String): Column
  def orderBy(df: A, columns: Seq[Column]): A
  def intersect(df: A, other: A): A
  def map[U: Encoder](df: A, fn: Row => U): A
  def flatMap[U: Encoder](df: A, fn: Row => TraversableOnce[U]): A
  def collect(df: A): Array[Row]
  def show(df: A, truncate: Boolean): Unit
  def toDataFrame(df: A): DataFrame
}

object Relational extends RelationalInstances {
  sealed trait Untyped
  sealed trait Typed
}

trait RelationalInstances {

  import Relational._

  /** [[Relational]] implementation for untyped dataframes.  In our case,
    * untyped dataframes means dataframes with [[org.apache.spark.sql.types.StringType]]
    * columns, instead of [[org.apache.spark.sql.types.StructType]].
    *
    * @param sc a SQLContext for low level Spark operations
    * @return
    */
  implicit val untypedDataFrameRelational: Relational[DataFrame @@ Untyped] =
    new Relational[DataFrame @@ Untyped] {
      def empty(implicit sc: SQLContext): DataFrame @@ Untyped = @@(
        sc.emptyDataFrame
      )

      def isEmpty(df: DataFrame @@ Untyped): Boolean =
        df.unwrap.isEmpty

      def crossJoin(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped
      ): DataFrame @@ Untyped =
        @@(left.unwrap.crossJoin(right.unwrap))

      def innerJoin(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped,
          columns: Seq[String]
      ): DataFrame @@ Untyped =
        @@(left.unwrap.join(right.unwrap, columns, "inner"))

      def leftJoin(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped,
          columns: Seq[String]
      ): DataFrame @@ Untyped =
        @@(left.unwrap.join(right.unwrap, columns, "left"))

      def innerJoin(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped,
          expression: Column
      ): DataFrame @@ Untyped =
        @@(left.unwrap.join(right.unwrap, expression, "inner"))

      def leftJoin(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped,
          expression: Column
      ): DataFrame @@ Untyped =
        @@(left.unwrap.join(right.unwrap, expression, "left"))

      def leftSemi(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped,
          columns: Seq[String]
      ): DataFrame @@ Untyped = @@ {
        left.unwrap.join(right.unwrap, columns, "leftsemi")
      }

      def leftAnti(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped,
          columns: Seq[String]
      ): DataFrame @@ Untyped = @@ {
        left.unwrap.join(right.unwrap, columns, "leftanti")
      }

      def leftOuter(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped,
          columns: Seq[String]
      ): DataFrame @@ Untyped = @@ {
        left.unwrap.join(right.unwrap, columns, "left_outer")
      }

      def union(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped
      ): DataFrame @@ Untyped =
        @@(left.unwrap.union(right.unwrap))

      def unionByName(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped
      ): DataFrame @@ Untyped = @@ {
        left.unwrap.unionByName(right.unwrap)
      }

      def minus(
          left: DataFrame @@ Untyped,
          right: DataFrame @@ Untyped
      ): DataFrame @@ Untyped =
        @@(left.unwrap.exceptAll(right.unwrap))

      def offset(df: DataFrame @@ Untyped, offset: Long)(implicit
          sc: SQLContext
      ): DataFrame @@ Untyped =
        @@ {
          val rdd = df.unwrap.rdd.zipWithIndex
            .filter { case (_, idx) => idx >= offset }
            .map(_._1)
          sc.createDataFrame(rdd, df.unwrap.schema)
        }

      def limit(df: DataFrame @@ Untyped, l: Long): DataFrame @@ Untyped =
        @@ {
          df.unwrap.limit(l.toInt)
        }

      def filter(
          df: DataFrame @@ Untyped,
          condition: Column
      ): DataFrame @@ Untyped = @@ {
        df.unwrap.filter(condition)
      }

      def distinct(df: DataFrame @@ Untyped): DataFrame @@ Untyped = @@ {
        df.unwrap.distinct()
      }

      def drop(df: DataFrame @@ Untyped, column: String): DataFrame @@ Untyped =
        @@ {
          df.unwrap.drop(column)
        }

      def withColumn(
          df: DataFrame @@ Untyped,
          name: String,
          expression: Column
      ): DataFrame @@ Untyped = @@ {
        df.unwrap.withColumn(name, expression)
      }

      def withColumnRenamed(
          df: DataFrame @@ Untyped,
          oldName: String,
          newName: String
      ): DataFrame @@ Untyped = @@ {
        df.unwrap.withColumnRenamed(oldName, newName)
      }

      def columns(df: DataFrame @@ Untyped): Array[String] =
        df.unwrap.columns

      def select(
          df: DataFrame @@ Untyped,
          columns: Seq[Column]
      ): DataFrame @@ Untyped =
        @@ {
          df.unwrap.select(columns: _*)
        }

      def schema(df: DataFrame @@ Untyped): StructType =
        df.unwrap.schema

      def fromDataFrame(df: DataFrame): DataFrame @@ Untyped =
        @@(df)

      def getColumn(df: DataFrame @@ Untyped, col: String): Column =
        df.unwrap(col)

      def orderBy(
          df: DataFrame @@ Untyped,
          columns: Seq[Column]
      ): DataFrame @@ Untyped = @@ {
        df.unwrap.orderBy(columns: _*)
      }

      def intersect(
          df: DataFrame @@ Untyped,
          other: DataFrame @@ Untyped
      ): DataFrame @@ Untyped = @@ {
        df.unwrap.intersect(other.unwrap)
      }

      def map[U: Encoder](
          df: DataFrame @@ Untyped,
          fn: Row => U
      ): DataFrame @@ Untyped =
        df.unwrap.map[U](fn).toDF.@@

      def flatMap[U: Encoder](
          df: DataFrame @@ Untyped,
          fn: Row => TraversableOnce[U]
      ): DataFrame @@ Untyped =
        df.unwrap.flatMap(fn).toDF.@@

      def collect(df: DataFrame @@ Untyped): Array[Row] =
        df.unwrap.collect()

      def show(df: DataFrame @@ Untyped, truncate: Boolean): Unit =
        df.unwrap.show(truncate)

      def toDataFrame(df: DataFrame @@ Untyped): DataFrame =
        df.unwrap
    }
}

/** types of the RelationalGrouped typeclass can be grouped in a relational fashion.
  * RelationalGrouped uses an associated type B for the return type of the groupBy operation.
  *
  * Since it uses a functional dependency, it's easier to use the [[RelationalGrouped.Aux]] type
  * for type hinting.
  *
  * {{{
  * def df[A: Relational]: Relational[A] = ???
  *
  * def grouped[A, B]: RelationalGrouped.Aux[A, B] = RelationalGrouped.Aux[A, B].groupby(df, columns)
  * }}}
  *
  * @tparam DS
  */
trait RelationalGrouped[DS] {
  type B // scalastyle:off
  def groupBy(df: DS, columns: Seq[Column]): B
}

object RelationalGrouped extends RelationalGroupedInstances {

  type Aux[A, B0] = RelationalGrouped[A] {
    type B = B0
  }
  object Aux {
    def apply[A, B](implicit
        A: RelationalGrouped.Aux[A, B]
    ): RelationalGrouped.Aux[A, B] = A
  }

  def apply[A](implicit
      A: RelationalGrouped[A]
  ): RelationalGrouped[A] = A

  implicit class Ops[A](relational: A)(implicit A: RelationalGrouped[A]) {
    def groupBy(columns: Seq[Column]): RelationalGrouped[A]#B =
      RelationalGrouped[A].groupBy(relational, columns)
  }
}

trait RelationalGroupedInstances {
  implicit val relationalGroupedDataset: RelationalGrouped.Aux[
    DataFrame @@ Untyped,
    RelationalGroupedDataset @@ Untyped
  ] = new RelationalGrouped[
    DataFrame @@ Untyped
  ] {

    type B = RelationalGroupedDataset @@ Untyped

    override def groupBy(
        df: DataFrame @@ Untyped,
        columns: Seq[Column]
    ): RelationalGroupedDataset @@ Untyped =
      @@(df.unwrap.groupBy(columns: _*))
  }
}
