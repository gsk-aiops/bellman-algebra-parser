package com.gsk.kg.engine

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

import com.gsk.kg.config.Config

object RdfFormatter {

  /** This function reformats a dataframe as per RDF standards. In the
    * [[formatField]] helper function we apply some heuristics to identify the
    * kind of RDF node we should format to.
    *
    * @param df
    * @return
    */
  def formatDataFrame(df: DataFrame, config: Config): DataFrame = {
    val formatted = if (config.formatRdfOutput && !config.typeDataframe) {
      df.columns.foldLeft(df) { (d, column) =>
        d.withColumn(column, format(col(column)))
      }
    } else if (config.formatRdfOutput && config.typeDataframe) {
      df.columns.foldLeft(df) { (d, column) =>
        d.withColumn(column, prettyPrintTypedColumn(col(column)))
      }
    } else {
      df
    }

    if (config.stripQuestionMarksOnOutput) {
      removeDataFrameColumnsQuestionMarks(formatted)
    } else {
      formatted
    }
  }

  def prettyPrintTypedColumn(col: Column): Column =
    when(
      col("type") === RdfType.Uri.repr,
      concat(
        lit(Tokens.openAngleBracket),
        col("value"),
        lit(Tokens.closingAngleBracket)
      )
    ).when(
      col("type") === RdfType.String.repr && col("lang").isNotNull,
      concat(
        lit(Tokens.doubleQuote),
        col("value"),
        lit(Tokens.doubleQuote),
        lit(Tokens.langAnnotation),
        col("lang")
      )
    ).when(
      col("type") === RdfType.String.repr && isNull(col("lang")),
      concat(
        lit(Tokens.doubleQuote),
        col("value"),
        lit(Tokens.doubleQuote)
      )
    ).when(
      col("type") === RdfType.Blank.repr,
      concat(lit(Tokens.blankNode), col("value"))
    ).otherwise(
      concat(
        lit(Tokens.doubleQuote),
        col("value"),
        lit(Tokens.doubleQuote),
        lit(Tokens.typeAnnotation),
        lit(Tokens.openAngleBracket),
        lit(col("type")),
        lit(Tokens.closingAngleBracket)
      )
    )

  private def removeDataFrameColumnsQuestionMarks(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { case (acc, column) =>
      if (column.startsWith("?")) {
        acc.withColumnRenamed(column, column.replace("?", ""))
      } else {
        acc
      }
    }
  }

  private def format(col: Column): Column = {
    when(
      isBoolean(col),
      col.cast(DataTypes.StringType)
    ).when(
      isLocalizedString(col),
      col.cast(DataTypes.StringType)
    ).when(
      isUri(col),
      when(
        col.startsWith("<") && col.endsWith(">"),
        col.cast(DataTypes.StringType)
      ).otherwise(format_string("<%s>", col))
    ).when(
      isBlank(col),
      col.cast(DataTypes.StringType)
    ).when(
      isDatatypeLiteral(col),
      col.cast(DataTypes.StringType)
    ).when(
      isNumber(col),
      col.cast(DataTypes.StringType)
    ).when(
      isNull(col),
      col.cast(DataTypes.StringType)
    ).otherwise(
      when(
        isQuoted(col),
        col.cast(DataTypes.StringType)
      ).otherwise(
        format_string("\"%s\"", col)
      )
    )
  }

  def isDatatypeLiteral(column: Column): Column =
    column.startsWith("\"") && column.contains("\"^^")

  def isBlank(column: Column): Column =
    column.startsWith("_:")

  def isBoolean(column: Column): Column =
    column === lit("true") || column === lit("false")

  def isUri(column: Column): Column =
    column.startsWith("<") && column.endsWith(">") ||
      column.startsWith("http://") ||
      column.startsWith("https://") ||
      column.startsWith("mailto:")

  def isNumber(column: Column): Column =
    column.cast(DataTypes.DoubleType).isNotNull ||
      column.cast(DataTypes.FloatType).isNotNull ||
      column.cast(DataTypes.LongType).isNotNull ||
      column.cast(DataTypes.ShortType).isNotNull ||
      column.cast(DataTypes.IntegerType).isNotNull

  def isNull(column: Column): Column =
    column === lit("null") || column.isNull

  def isLocalizedString(column: Column): Column =
    column.contains("\"@")

  def isQuoted(column: Column): Column =
    column.startsWith("\"") && column.endsWith("\"")
}
