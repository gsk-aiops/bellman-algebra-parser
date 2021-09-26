package com.gsk.kg.engine

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SQLContext

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.EngineError

package object syntax extends DataFrameSyntax with ColumnSyntax

trait ColumnSyntax {
  implicit class TypedColumnOps(private val col: Column) {
    def `type`: Column = col("type")
    def value: Column  = col("value")
    def lang: Column   = col("lang")

    def hasType(tpe: RdfType): Column =
      `type` === tpe.repr

    def map(fn: Column => Column): Column =
      DataFrameTyper.createRecord(
        tpe = col.`type`,
        value = fn(col.value),
        lang = col.lang
      )
  }
}

trait DataFrameSyntax {
  implicit class ToTypedDFString[T: Encoder](private val lst: Seq[T])(implicit
      sc: SQLContext
  ) {
    import sc.implicits._

    def toTypedDF(): DataFrame =
      toTypedDF()

    def toTypedDF(colNames: String*): DataFrame =
      DataFrameTyper.typeDataFrame(
        lst.toDF(colNames: _*),
        Config.default.copy(typeDataframe = true)
      )
  }

  implicit class SparQLSyntaxOnDataFrame(private val df: DataFrame)(implicit
      sc: SQLContext
  ) {

    /** Compile query with dataframe with provided configuration
      * @param query
      * @param config
      * @return
      */
    def sparql(query: String, config: Config): DataFrame =
      Compiler.compile(df, query, config) match {
        case Left(a)  => throw EngineException(a)
        case Right(b) => b
      }

    /** Compile query with dataframe with default configuration
      * @param query
      * @return
      */
    def sparql(query: String): DataFrame =
      sparql(query, Config.default)
  }

  final case class EngineException(error: EngineError)
      extends RuntimeException(error.toString())

}
