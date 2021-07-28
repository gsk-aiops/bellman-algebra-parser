package com.gsk.kg.engine.rdf

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat => cc, _}
import org.apache.spark.sql.types.BooleanType
import cats.data.NonEmptyList

object TypedFuncs {

  def `if`(cnd: Column, ifTrue: Column, ifFalse: Column): Column = {
    val ebv = effectiveBooleanValue(cnd)
    when(
      ebv.isNotNull,
      when(
        ebv,
        ifTrue
      ).otherwise(ifFalse)
    ).otherwise(lit(null)) // scalastyle:off
  }

  def effectiveBooleanValue(col: Column): Column = {
    lazy val ifNumeric = when(
      col("value").cast("double").isNull,
      lit(false)
    ).otherwise {
      val double = col("value").cast("double")
      when(
        isnan(double) || double === lit(0.0),
        lit(false)
      ).otherwise(lit(true))
    }

    lazy val ifStringLit = when(
      col("value") === lit(""),
      lit(false)
    ).otherwise(lit(true))

    def isBoolean(col: Column): Column =
      col("type") === RdfType.Boolean.repr

    def isString(col: Column): Column =
      col("type") === RdfType.String.repr

    def isNumeric(col: Column): Column =
      isInt(col) || isDouble(col) || isDecimal(col)

    when(
      isBoolean(col),
      col("value").cast(BooleanType)
    ).when(
      isNumeric(col),
      ifNumeric
    ).when(
      isString(col),
      ifStringLit
    ).otherwise(lit(null)) // scalastyle:off
  }

  def isInt(col: Column): Column =
    col("type") === RdfType.Int.repr

  def isDouble(col: Column): Column =
    col("type") === RdfType.Double.repr

  def isDecimal(col: Column): Column =
    col("type") === RdfType.Decimal.repr

  def promoteNumericArgsToNumericResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    when( // Int, Int => Int
      isInt(col1) && isInt(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Int.repr
      )
    ).when( // Int, Decimal => Decimal
      isInt(col1) && isDecimal(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Decimal.repr
      )
    ).when( // Int, Double => Double
      isInt(col1) && isDouble(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Double.repr
      )
    ).when( // Decimal, Int => Decimal
      isDecimal(col1) && isInt(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Decimal.repr
      )
    ).when( // Decimal, Decimal => Decimal
      isDecimal(col1) && isDecimal(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Decimal.repr
      )
    ).when( // Decimal, Double => Double
      isDecimal(col1) && isDouble(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Double.repr
      )
    ).when( // Double, Int => Double
      isDouble(col1) && isInt(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Double.repr
      )
    ).when( // Double, Decimal => Double
      isDouble(col1) && isDecimal(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Double.repr
      )
    ).when( // Double, Double => Double
      isDouble(col1) && isDouble(col2),
      Typer.createRecord(
        value = op(col1("value"), col2("value")),
        tpe = RdfType.Double.repr
      )
    )
  }

  def add(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ + _)

  def subtract(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ - _)

  def multiply(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ * _)

  def divide(l: Column, r: Column): Column =
    promoteNumericArgsToNumericResult(l, r)(_ / _)

  def uri(col: Column): Column =
    when(
      col("type") === RdfType.Uri.repr,
      col
    ).when(
      col("type") === RdfType.String.repr,
      Typer.createRecord(format_string("<%s>", col("value")), RdfType.Uri.repr)
    )

  def concat(appendTo: Column, append: NonEmptyList[Column]): Column = {
    val concatValues = append.toList.foldLeft(appendTo("value")) {
      case (acc, elem) =>
        cc(acc, elem("value"))
    }

    when(
      areAllArgsSameTypeAndSameTags(appendTo, append.toList),
      when(
        appendTo("lang").isNotNull,
        Typer.createRecord(concatValues, RdfType.String.repr, appendTo("lang"))
      ).otherwise(
        Typer.createRecord(concatValues, RdfType.String.repr)
      )
    ).otherwise(
      Typer.createRecord(concatValues, RdfType.String.repr)
    )
  }

  private def areAllArgsSameTypeAndSameTags(
      arg1: Column,
      args: List[Column]
  ): Column = {
    when(
      arg1("lang").isNotNull, {
        args.foldLeft(lit(true)) { case (acc, elem) =>
          acc && when(
            elem("lang").isNotNull,
            arg1("lang") === elem("lang")
          ).otherwise(lit(false))
        }
      }
    ).otherwise(
      args.foldLeft(lit(true)) { case (acc, elem) =>
        acc && arg1("type") === elem("type")
      }
    )
  }

}
