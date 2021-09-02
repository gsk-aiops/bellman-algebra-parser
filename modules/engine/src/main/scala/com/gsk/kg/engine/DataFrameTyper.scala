package com.gsk.kg.engine

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.gsk.kg.config.Config
import com.gsk.kg.engine.functions.Literals
import com.gsk.kg.engine.functions.Literals.LocalizedLiteral
import com.gsk.kg.engine.functions.Literals.TypedLiteral

object Tokens {
  val openAngleBracket = "<"
  val closingAngleBracket = ">"
  val doubleQuote = "\""
  val typeAnnotation = "^^"
  val langAnnotation = "@"
  val blankNode = "_:"
}

trait RdfType {
  val repr: Column
}

object RdfType {
  case object String extends RdfType {
    val repr = lit("http://www.w3.org/2001/XMLSchema#string")
  }

  case object Uri extends RdfType {
    val repr = lit("uri")
  }

  case object Boolean extends RdfType {
    val repr = lit("http://www.w3.org/2001/XMLSchema#boolean")
  }

  case object Int extends RdfType {
    val repr = lit("http://www.w3.org/2001/XMLSchema#integer")
  }

  case object Double extends RdfType {
    val repr = lit("http://www.w3.org/2001/XMLSchema#double")
  }

  case object Decimal extends RdfType {
    val repr = lit("http://www.w3.org/2001/XMLSchema#decimal")
  }

  case object Blank extends RdfType {
    val repr = lit("blank")
  }

  final case class Literal(str: String) extends RdfType {
    val repr = lit(str)
  }
}

object DataFrameTyper {
  def typeDataFrame(inDf: DataFrame, config: Config): DataFrame =
    if (config.typeDataframe) {
      applyTransformationToDF(inDf, parse)
    } else {
      inDf
    }

  val structSchema: StructType = new StructType()
    .add("value", StringType)
    .add("lang", StringType, nullable = true)
    .add("type", StringType)

  def applyTransformationToDF(
                               df: DataFrame,
                               transform: Column => Column
                             ): DataFrame =
    df.columns.foldLeft(df) { case (acc, column) =>
      acc.withColumn(column, transform(df(column)))
    }

  def parse(col: Column): Column =
    when(
      RdfFormatter.isUri(col),
      createRecord(
        rtrim(ltrim(col, Tokens.openAngleBracket), Tokens.closingAngleBracket),
        RdfType.Uri.repr
      )
    ).when(
      TypedLiteral.isTypedLiteral(col),
      createRecord(
        Literals.extractStringLiteral(col),
        rtrim(
          ltrim(TypedLiteral(col).tag, Tokens.openAngleBracket),
          Tokens.closingAngleBracket
        )
      )
    ).when(
      RdfFormatter.isLocalizedString(col),
      createRecord(
        value = LocalizedLiteral(col).value,
        tpe = RdfType.String.repr,
        lang = LocalizedLiteral(col).tag
      )
    ).when(
      col.startsWith(Tokens.doubleQuote) && col.endsWith(Tokens.doubleQuote),
      createRecord(
        value = rtrim(ltrim(col, Tokens.doubleQuote), Tokens.doubleQuote),
        tpe = RdfType.String.repr
      )
    ).when(
      RdfFormatter.isBoolean(col),
      createRecord(
        value = col,
        tpe = RdfType.Boolean.repr
      )
    ).when(
      RdfFormatter.isNumber(col) && col.contains("e"),
      createRecord(
        value = col,
        tpe = RdfType.Double.repr
      )
    ).when(
      RdfFormatter.isNumber(col) && col.contains("."),
      createRecord(
        value = col,
        tpe = RdfType.Decimal.repr
      )
    ).when(
      RdfFormatter.isNumber(col),
      createRecord(
        value = col,
        tpe = RdfType.Int.repr
      )
    ).when(
      RdfFormatter.isBlank(col),
      createRecord(
        ltrim(col, Tokens.blankNode),
        RdfType.Blank.repr
      )
    )

  // scalafix:off
  def createRecord(
                    value: Column,
                    tpe: Column,
                    lang: Column = lit(null) // scalastyle:off
                  ): Column =
    struct(
      value.as("value"),
      tpe.as("type"),
      lang.as("lang")
    )
}
