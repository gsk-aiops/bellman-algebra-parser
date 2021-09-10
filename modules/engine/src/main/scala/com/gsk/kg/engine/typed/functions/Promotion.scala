package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types._

import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.syntax._

object Promotion {

  trait Typed[T] {
    type Return

    def rdfType: RdfType
  }

  object Typed {
    def instance[T](tpe: RdfType): Typed[T] = new Typed[T] { def rdfType = tpe }

    def apply[T](implicit T: Typed[T]): Typed[T] = T

    implicit val StringTyped: Typed[String]   = instance(RdfType.String)
    implicit val DoubleTyped: Typed[Double]   = instance(RdfType.Double)
    implicit val IntTyped: Typed[Int]         = instance(RdfType.Int)
    implicit val BooleanTyped: Typed[Boolean] = instance(RdfType.Boolean)
    implicit val FloatTyped: Typed[Float]     = instance(RdfType.Float)
  }

  def promote[T: Typed](col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val Precision = 10
    val Scale     = 2

    when(
      col1.hasType(RdfType.Double) || col2.hasType(RdfType.Double),
      Typed[T].rdfType(
        op(col1.value.cast(DoubleType), col2.value.cast(DoubleType))
      )
    ).when(
      col1.hasType(RdfType.Float) || col2.hasType(RdfType.Float),
      Typed[T].rdfType(
        op(col1.value.cast(FloatType), col2.value.cast(FloatType))
      )
    ).when(
      col1.hasType(RdfType.Numeric) || col2.hasType(RdfType.Numeric),
      Typed[T].rdfType(
        op(col1.value.cast(DoubleType), col2.value.cast(DoubleType))
      )
    ).when(
      col1.hasType(RdfType.Decimal) || col2.hasType(RdfType.Decimal),
      Typed[T].rdfType(
        op(
          col1.value.cast(DecimalType(Precision, Scale)),
          col2.value.cast(DecimalType(Precision, Scale))
        )
      )
    ).when(
      col1.hasType(RdfType.Int) || col2.hasType(RdfType.Int),
      Typed[T]
        .rdfType(op(col1.value.cast(IntegerType), col2.value.cast(IntegerType)))
    )
  }
}
