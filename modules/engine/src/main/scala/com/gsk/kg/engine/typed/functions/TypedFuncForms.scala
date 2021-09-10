package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType

import com.gsk.kg.engine.DataFrameTyper
import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.syntax.TypedColumnOps
import com.gsk.kg.engine.typed.functions.TypedLiterals._

object TypedFuncForms {

  /** Performs logical binary operation '==' over two columns
    * @param l
    * @param r
    * @return
    */
  def equals(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ === _)
      .otherwise(
        promoteNumericArgsToBooleanResult(l, r)(_ === _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ === _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ === _)
                  .otherwise(RdfType.Boolean(l === r))
              )
          )
      )

  /** Peforms logical binary operation '>' over two columns
    * @param l
    * @param r
    * @return
    */
  def gt(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ > _)
      .otherwise(
        promoteNumericArgsToBooleanResult(l, r)(_ > _)
          otherwise (
            promoteStringArgsToBooleanResult(l, r)(_ > _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ > _)
                  .otherwise(RdfType.Boolean(l > r))
              )
          )
      )

  /** Performs logical binary operation '<' over two columns
    * @param l
    * @param r
    * @return
    */
  def lt(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ < _)
      .otherwise(
        promoteNumericArgsToBooleanResult(l, r)(_ < _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ < _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ < _)
                  .otherwise(RdfType.Boolean(l < r))
              )
          )
      )

  /** Performs logical binary operation '<=' over two columns
    * @param l
    * @param r
    * @return
    */
  def gte(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ >= _)
      .otherwise(
        promoteNumericArgsToBooleanResult(l, r)(_ >= _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ >= _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ >= _)
                  .otherwise(RdfType.Boolean(l >= r))
              )
          )
      )

  /** Performs logical binary operation '>=' over two columns
    * @param l
    * @param r
    * @return
    */
  def lte(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ <= _)
      .otherwise(
        promoteNumericArgsToBooleanResult(l, r)(_ <= _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ <= _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ <= _)
                  .otherwise(RdfType.Boolean(l <= r))
              )
          )
      )

  /** Performs logical binary operation 'or' over two columns
    * @param l
    * @param r
    * @return
    */
  def or(l: Column, r: Column): Column =
    RdfType.Boolean(l.value || r.value)

  /** Performs logical binary operation 'and' over two columns
    * @param r
    * @param l
    * @return
    */
  def and(l: Column, r: Column): Column =
    RdfType.Boolean(l.value && r.value)

  /** Negates all rows of a column
    * @param s
    * @return
    */
  def negate(s: Column): Column =
    RdfType.Boolean(not(s.value.cast(BooleanType)).cast(StringType))

  /** The IN operator tests whether the RDF term on the left-hand side is found in the values of list of expressions
    * on the right-hand side. The test is done with "=" operator, which tests for the same value, as determined by
    * the operator mapping.
    *
    * A list of zero terms on the right-hand side is legal.
    *
    * Errors in comparisons cause the IN expression to raise an error if the RDF term being tested is not found
    * elsewhere in the list of terms.
    * @param e
    * @param xs
    * @return
    */
  def in(e: Column, xs: List[Column]): Column =
    RdfType.Boolean(e.value.isInCollection(xs.map(_.value)))
//
//    val anyEqualsExpr = xs.foldLeft(RdfType.Boolean.False) { case (acc, x) =>
//      or(acc, equals(e, x))
//    }
//
//    val anyIsNull = xs.foldLeft(RdfType.Boolean.False) { case (acc, x) =>
//      or(acc, x.typedIsNull())
//    }
//
//    e.isInCollection(xs)
//
//    when(
//      anyEqualsExpr.value,
//      RdfType.Boolean.True
//    ).when(
//      anyIsNull.value,
//      TypedLiterals.nullLiteral
//    ).otherwise(RdfType.Boolean.False)

  /** Returns TRUE if term1 and term2 are the same RDF term
    * @param l
    * @param r
    * @return
    */
  def sameTerm(l: Column, r: Column): Column = {

    val leftAndRightLocalized = {
      when(
        l.lang === r.lang,
        RdfType.Boolean(l.value === r.value)
      ).otherwise(RdfType.Boolean.False)
    }

    when(
      l.lang.isNotNull && r.lang.isNotNull,
      leftAndRightLocalized
    ).when(
      l.lang.isNotNull || r.lang.isNotNull,
      RdfType.Boolean(l.value === r.value && l.`type` === r.`type`)
    ).otherwise(RdfType.Boolean(l === r))
  }

  /** The IF function form evaluates the first argument, interprets it as a effective boolean value,
    * then returns the value of expression2 if the EBV is true, otherwise it returns the value of expression3.
    * Only one of expression2 and expression3 is evaluated. If evaluating the first argument raises an error,
    * then an error is raised for the evaluation of the IF expression.
    * @param cnd
    * @param ifTrue
    * @param ifFalse
    * @return
    */
  def `if`(cnd: Column, ifTrue: Column, ifFalse: Column): Column = {
    val ebv = effectiveBooleanValue(cnd)

    when(
      ebv.value.cast(BooleanType),
      ifTrue
    ).otherwise(ifFalse)
  }

  /** Effective boolean value is used to calculate the arguments to the logical functions logical-and, logical-or,
    * and fn:not, as well as evaluate the result of a FILTER expression.
    *
    * The XQuery Effective Boolean Value rules rely on the definition of XPath's fn:boolean.
    * The following rules reflect the rules for fn:boolean applied to the argument types present in SPARQL queries:
    *
    * - The EBV of any literal whose type is xsd:boolean or numeric is false if the lexical form is not valid for
    * that datatype (e.g. "abc"^^xsd:integer).
    * - If the argument is a typed literal with a datatype of xsd:boolean, and it has a valid lexical form,
    * the EBV is the value of that argument.
    * - If the argument is a plain literal or a typed literal with a datatype of xsd:string,
    * the EBV is false if the operand value has zero length; otherwise the EBV is true.
    * - If the argument is a numeric type or a typed literal with a datatype derived from a numeric type,
    * and it has a valid lexical form, the EBV is false if the operand value is NaN or is numerically equal to zero;
    * otherwise the EBV is true.
    * - All other arguments, including unbound arguments, produce a type error.
    *
    * An EBV of true is represented as a typed literal with a datatype of xsd:boolean and a lexical value of "true";
    * an EBV of false is represented as a typed literal with a datatype of xsd:boolean and a lexical value of "false".
    * @param column
    * @return
    */
  def effectiveBooleanValue(col: Column): Column = {
    lazy val ifBooleanLit = when(
      col.value =!= "true" && col.value =!= "false",
      RdfType.Boolean.False
    ).otherwise(
      col
    )

    lazy val ifNumericLit = when(
      col.value.cast("double").isNull,
      RdfType.Boolean.False
    ).otherwise {
      val double = col.value.cast("double")
      when(
        isnan(double) || double === 0.0,
        RdfType.Boolean.False
      ).otherwise(RdfType.Boolean.True)
    }

    lazy val ifStringLit = when(
      col.value === lit(""),
      RdfType.Boolean.False
    ).otherwise(RdfType.Boolean.True)

    when(
      isBooleanLiteral(col),
      ifBooleanLit
    ).when(
      isNumericLiteral(col),
      ifNumericLit
    ).when(
      isStringLiteral(col),
      ifStringLit
    )
  }

  /** Returns true if var is bound to a value. Returns false otherwise. Variables with the value NaN or INF
    * are considered bound.
    * @param col
    * @return
    */
  def bound(col: Column): Column =
    when(col.hasType(RdfType.Null) || col.isNull, RdfType.Boolean.False)
      .otherwise(RdfType.Boolean.True)

  /** The COALESCE function form returns the RDF term value of the first expression that evaluates without error.
    * In SPARQL, evaluating an unbound variable raises an error.
    * If none of the arguments evaluates to an RDF term, an error is raised.
    * If no expressions are evaluated without error, an error is raised.
    * @param cols
    * @return
    */
  def coalesce(cols: List[Column]): Column = {
    cols.foldLeft(DataFrameTyper.NullLiteral) { case (acc, elem) =>
      when(
        elem.hasType(RdfType.Null),
        acc
      ).when(
        acc.hasType(RdfType.Null),
        elem
      ).otherwise(acc)
    }
  }
}
