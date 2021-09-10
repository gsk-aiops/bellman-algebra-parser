package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat => _, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType

import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.syntax._

object TypedLiterals {

  object NumericLiteral {

    object BooleanResult {

      def doNotPromote(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value, col2.value))

      def promoteLeftInt(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value.cast(IntegerType), col2.value))

      def promoteLeftDecimal(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value.cast("decimal"), col2.value))

      def promoteLeftFloat(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value.cast(FloatType), col2.value))

      def promoteLeftDouble(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value.cast(DoubleType), col2.value))

      def promoteRightInt(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value, col2.value.cast(IntegerType)))

      def promoteRightDecimal(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value.cast("decimal"), col2.value))

      def promoteRightFloat(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value.cast(FloatType), col2.value))

      def promoteRightDouble(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column =
        RdfType.Boolean(op(col1.value.cast(DoubleType), col2.value))
    }
  }

  def isPlainNumericFloatingPoint(col: Column): Column =
    col.hasType(RdfType.Double)

  def isPlainNumericNotFloatingPoint(col: Column): Column =
    col.hasType(RdfType.Int)

  def isStringLiteral(col: Column): Column =
    col.hasType(RdfType.String)

  def isBooleanLiteral(col: Column): Column =
    col.hasType(RdfType.Boolean)

  def isNumericLiteral(col: Column): Column =
    isIntNumericLiteral(col) ||
      isDecimalNumericLiteral(col) ||
      isFloatNumericLiteral(col) ||
      isDoubleNumericLiteral(col)

  def isNumericNumericLiteral(col: Column): Column =
    col.hasType(RdfType.Numeric)

  def isIntNumericLiteral(col: Column): Column =
    col.hasType(RdfType.Int)

  def isDecimalNumericLiteral(col: Column): Column =
    col.hasType(RdfType.Decimal)

  def isFloatNumericLiteral(col: Column): Column =
    col.hasType(RdfType.Float)

  def isDoubleNumericLiteral(col: Column): Column =
    col.hasType(RdfType.Double)

  def isDateTimeLiteral(col: Column): Column =
    col.hasType(RdfType.DateTime)

  def applyBooleanResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column =
    RdfType.Boolean(op(col1.value, col2.value))

  // scalastyle:off
  def promoteNumericArgsToNumericResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column =
    when(
      isDoubleNumericLiteral(col1) || isDoubleNumericLiteral(col2),
      RdfType.Double(op(col1.value, col2.value))
    ).when(
      isFloatNumericLiteral(col1) || isFloatNumericLiteral(col2),
      RdfType.Float(op(col1.value, col2.value))
    ).when(
      isDecimalNumericLiteral(col1) || isDecimalNumericLiteral(col2),
      RdfType.Decimal(op(col1.value, col2.value))
    ).when(
      isIntNumericLiteral(col1) || isIntNumericLiteral(col2),
      RdfType.Int(op(col1.value, col2.value))
    )
  // scalastyle:on

  def promoteNumericArgsToBooleanResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column =
    Promotion.promote[Boolean](col1, col2)(op)

  def promoteStringArgsToBooleanResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column =
    when(
      col1.hasType(RdfType.String) && col2.hasType(RdfType.String),
      RdfType.Boolean(op(col1, col2))
    )

  def promoteBooleanBooleanToBooleanResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column =
    when(
      isBooleanLiteral(col1) && isBooleanLiteral(col2),
      RdfType.Boolean(op(col1.value, col2.value))
    ).when(
      !isBooleanLiteral(col1) && isBooleanLiteral(col2),
      RdfType.Boolean(op(col1.value, col2.value))
    ).when(
      isBooleanLiteral(col1) && !isBooleanLiteral(col2),
      RdfType.Boolean(op(col1.value, col2.value))
    )

  object DateLiteral {

    /** This helper method tries to parse a datetime expressed as a RDF
      * datetime string `"0193-07-03T20:50:09.000+04:00"^^xsd:dateTime`
      * to a column with underlying type datetime.
      *
      * @param col
      * @return
      */
    def parseDateFromRDFDateTime(col: Column): Column =
      RdfType.SparkType(TimestampType)(to_timestamp(col.value))

    def applyDateTimeLiteral(l: Column, r: Column)(
        operator: (Column, Column) => Column
    ): Column =
      when(
        l.hasType(RdfType.DateTime) && r.hasType(RdfType.DateTime),
        RdfType.Boolean(
          operator(
            parseDateFromRDFDateTime(l),
            parseDateFromRDFDateTime(r)
          )
        )
      )
  }

  val nullLiteral: Column = RdfType.Null(lit(null)) // scalastyle:off
}
