package com.gsk.kg.engine

import cats.implicits._

import higherkindness.droste._
import higherkindness.droste.macros.deriveTraverse
import higherkindness.droste.syntax.all._
import higherkindness.droste.util.newtypes.@@

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import com.gsk.kg.config.Config
import com.gsk.kg.engine.properties.FuncProperty
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.PropertyExpression
import com.gsk.kg.sparqlparser.PropertyExpression._
import com.gsk.kg.sparqlparser.Result

import monocle.macros.Lenses

@deriveTraverse sealed trait PropertyExpressionF[+A]

object PropertyExpressionF {

  type ColOrDf = Either[Column, DataFrame @@ Untyped]

  def compile[T](
      t: T,
      config: Config
  )(implicit
      T: Basis[PropertyExpressionF, T],
      sc: SQLContext
  ): DataFrame @@ Untyped => Result[ColOrDf] =
    df => {
      val algebraM: AlgebraM[M, PropertyExpressionF, ColOrDf] =
        AlgebraM.apply[M, PropertyExpressionF, ColOrDf] {
          case AlternativeF(_, _) =>
            unknownPropertyPath("alternative")
          case ReverseF(_) =>
            unknownPropertyPath("reverse")
          case SeqExpressionF(_, _) =>
            unknownPropertyPath("seqExpression")
          case OneOrMoreF(e) =>
            M.liftF(FuncProperty.betweenNAndM(df, Some(1), None, e))
          case ZeroOrMoreF(e) =>
            M.liftF(FuncProperty.betweenNAndM(df, Some(0), None, e))
          case ZeroOrOneF(e) =>
            M.liftF(FuncProperty.betweenNAndM(df, Some(0), Some(1), e))
          case NotOneOfF(es) =>
            M.liftF(FuncProperty.notOneOf(df, es))
          case BetweenNAndMF(n, m, e) =>
            M.liftF(FuncProperty.betweenNAndM(df, Some(n), Some(m), e))
          case ExactlyNF(n, e) =>
            M.liftF(FuncProperty.betweenNAndM(df, Some(n), Some(n), e))
          case NOrMoreF(n, e) =>
            M.liftF(FuncProperty.betweenNAndM(df, Some(n), None, e))
          case BetweenZeroAndNF(n, e) =>
            M.liftF(FuncProperty.betweenNAndM(df, None, Some(n), e))
          case UriF(s) => FuncProperty.uri(df, s).pure[M]
        }

      val eval = scheme.cataM[M, PropertyExpressionF, T, ColOrDf](algebraM)

      eval(t).runA(config, df)
    }

  private def unknownPropertyPath(name: String): M[ColOrDf] =
    M.liftF[Result, Config, Log, DataFrame @@ Untyped, ColOrDf](
      EngineError.UnknownPropertyPath(name).asLeft[ColOrDf]
    )

  @Lenses final case class AlternativeF[A](pel: A, per: A)
      extends PropertyExpressionF[A]
  @Lenses final case class ReverseF[A](e: A) extends PropertyExpressionF[A]
  final case class SeqExpressionF[A](
      pel: A,
      per: A
  )                                             extends PropertyExpressionF[A]
  @Lenses final case class OneOrMoreF[A](e: A)  extends PropertyExpressionF[A]
  @Lenses final case class ZeroOrMoreF[A](e: A) extends PropertyExpressionF[A]
  @Lenses final case class ZeroOrOneF[A](e: A)  extends PropertyExpressionF[A]
  @Lenses final case class NotOneOfF[A](es: List[A])
      extends PropertyExpressionF[A]
  @Lenses final case class BetweenNAndMF[A](n: Int, m: Int, e: A)
      extends PropertyExpressionF[A]
  @Lenses final case class ExactlyNF[A](n: Int, e: A)
      extends PropertyExpressionF[A]
  @Lenses final case class NOrMoreF[A](n: Int, e: A)
      extends PropertyExpressionF[A]
  @Lenses final case class BetweenZeroAndNF[A](n: Int, e: A)
      extends PropertyExpressionF[A]
  @Lenses final case class UriF[A](s: String) extends PropertyExpressionF[A]

  def alternative[A](pel: A, per: A): PropertyExpressionF[A] =
    AlternativeF[A](pel, per)
  def reverse[A](pe: A): PropertyExpressionF[A] = ReverseF[A](pe)
  def seqExpression[A](pel: A, per: A): PropertyExpressionF[A] =
    SeqExpressionF[A](pel, per)
  def oneOrMore[A](pe: A): PropertyExpressionF[A]       = OneOrMoreF[A](pe)
  def zeroOrMore[A](pe: A): PropertyExpressionF[A]      = ZeroOrMoreF[A](pe)
  def zeroOrOne[A](pe: A): PropertyExpressionF[A]       = ZeroOrOneF[A](pe)
  def notOneOf[A](pes: List[A]): PropertyExpressionF[A] = NotOneOfF[A](pes)
  def betweenNAndM[A](n: Int, m: Int, pe: A): PropertyExpressionF[A] =
    BetweenNAndMF[A](n, m, pe)
  def exactlyN[A](n: Int, pe: A): PropertyExpressionF[A] = ExactlyNF[A](n, pe)
  def nOrMore[A](n: Int, pe: A): PropertyExpressionF[A]  = NOrMoreF[A](n, pe)
  def betweenZeroAndN[A](n: Int, pe: A): PropertyExpressionF[A] =
    BetweenZeroAndNF[A](n, pe)
  def uri[A](s: String): PropertyExpressionF[A] = UriF[A](s)

  def alternativeR[T: Embed[PropertyExpressionF, *]](pel: T, per: T): T =
    alternative[T](pel, per).embed
  def reverseR[T: Embed[PropertyExpressionF, *]](pe: T): T =
    reverse[T](pe).embed
  def seqExpressionR[T: Embed[PropertyExpressionF, *]](pel: T, per: T): T =
    seqExpression[T](pel, per).embed
  def oneOrMoreR[T: Embed[PropertyExpressionF, *]](pe: T): T =
    oneOrMore[T](pe).embed
  def zeroOrMoreR[T: Embed[PropertyExpressionF, *]](pe: T): T =
    zeroOrMore[T](pe).embed
  def notOneOfR[T: Embed[PropertyExpressionF, *]](pes: List[T]): T =
    notOneOf[T](pes).embed
  def betweenNAndMR[T: Embed[PropertyExpressionF, *]](
      n: Int,
      m: Int,
      pe: T
  ): T =
    betweenNAndM[T](n, m, pe).embed
  def exactlyNR[T: Embed[PropertyExpressionF, *]](n: Int, pe: T): T =
    exactlyN[T](n, pe).embed
  def nOrMoreR[T: Embed[PropertyExpressionF, *]](n: Int, pe: T): T =
    nOrMore[T](n, pe).embed
  def betweenZeroAndNR[T: Embed[PropertyExpressionF, *]](n: Int, pe: T): T =
    betweenZeroAndN[T](n, pe).embed
  def uriR[T: Embed[PropertyExpressionF, *]](s: String): T =
    uri[T](s).embed

  val algebra: Algebra[PropertyExpressionF, PropertyExpression] =
    Algebra[PropertyExpressionF, PropertyExpression] {
      case AlternativeF(pel, per)   => Alternative(pel, per)
      case ReverseF(e)              => Reverse(e)
      case SeqExpressionF(pel, per) => SeqExpression(pel, per)
      case OneOrMoreF(e)            => OneOrMore(e)
      case ZeroOrMoreF(e)           => ZeroOrMore(e)
      case ZeroOrOneF(e)            => ZeroOrOne(e)
      case NotOneOfF(es)            => NotOneOf(es)
      case BetweenNAndMF(n, m, e)   => BetweenNAndM(n, m, e)
      case ExactlyNF(n, e)          => ExactlyN(n, e)
      case NOrMoreF(n, e)           => NOrMore(n, e)
      case BetweenZeroAndNF(n, e)   => BetweenZeroAndN(n, e)
      case UriF(s)                  => Uri(s)
    }

  val coalgebra: Coalgebra[PropertyExpressionF, PropertyExpression] =
    Coalgebra[PropertyExpressionF, PropertyExpression] {
      case Alternative(pel, per)   => AlternativeF(pel, per)
      case Reverse(e)              => ReverseF(e)
      case SeqExpression(pel, per) => SeqExpressionF(pel, per)
      case OneOrMore(e)            => OneOrMoreF(e)
      case ZeroOrMore(e)           => ZeroOrMoreF(e)
      case ZeroOrOne(e)            => ZeroOrOneF(e)
      case NotOneOf(es)            => NotOneOfF(es)
      case BetweenNAndM(n, m, e)   => BetweenNAndMF(n, m, e)
      case ExactlyN(n, e)          => ExactlyNF(n, e)
      case NOrMore(n, e)           => NOrMoreF(n, e)
      case BetweenZeroAndN(n, e)   => BetweenZeroAndNF(n, e)
      case Uri(s)                  => UriF(s)
    }

  implicit val basis: Basis[PropertyExpressionF, PropertyExpression] =
    Basis.Default[PropertyExpressionF, PropertyExpression](
      algebra = algebra,
      coalgebra = coalgebra
    )
}
