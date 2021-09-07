package com.gsk.kg.engine.optimizer

import cats.Functor
import cats.implicits._

import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.{Project => _, _}
import com.gsk.kg.engine.Log
import com.gsk.kg.engine.M
import com.gsk.kg.engine.Phase
import com.gsk.kg.engine.PropertyExpressionF
import com.gsk.kg.engine.PropertyExpressionF._
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.sparqlparser.PropertyExpression
import com.gsk.kg.sparqlparser.PropertyExpression._
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

object PropertyPathRewrite {

  private def generateRndVariable() =
    VARIABLE(s"?_${java.util.UUID.randomUUID().toString}")

  private def toRecursive[F[_]: Functor, R: Embed[F, *]](
      attr: Attr[F, _]
  ): R = {
    def fix[A](attr: Attr[F, _]): Fix[F] =
      Fix(Functor[F].map(Attr.un(attr)._2)(fix))

    val fixed: Fix[F] = fix(attr)

    scheme[Fix].gcata(Embed[F, R].algebra.gather(Gather.cata)).apply(fixed)
  }

  def reverseCoalgebra(implicit
      P: Project[PropertyExpressionF, PropertyExpression]
  ): CVCoalgebra[PropertyExpressionF, PropertyExpression] =
    CVCoalgebra[PropertyExpressionF, PropertyExpression] {
      case Reverse(SeqExpression(pel, per)) =>
        SeqExpressionF(
          Coattr.pure(Reverse(pel)),
          Coattr.pure(Reverse(per))
        )
      case Reverse(Alternative(pel, per)) =>
        AlternativeF(
          Coattr.pure(Reverse(pel)),
          Coattr.pure(Reverse(per))
        )
      case Reverse(OneOrMore(e)) =>
        OneOrMoreF(
          Coattr.pure(Reverse(e))
        )
      case Reverse(ZeroOrMore(e)) =>
        ZeroOrMoreF(
          Coattr.pure(Reverse(e))
        )
      case Reverse(ZeroOrOne(e)) =>
        ZeroOrOneF(
          Coattr.pure(Reverse(e))
        )
      case Reverse(NotOneOf(es)) =>
        NotOneOfF(
          es.map(e =>
            Coattr.pure[PropertyExpressionF, PropertyExpression](Reverse(e))
          )
        )
      case Reverse(BetweenNAndM(n, m, e)) =>
        BetweenNAndMF(n, m, Coattr.pure(Reverse(e)))
      case Reverse(ExactlyN(n, e)) =>
        ExactlyNF(n, Coattr.pure(Reverse(e)))
      case Reverse(NOrMore(n, e)) =>
        NOrMoreF(n, Coattr.pure(Reverse(e)))
      case Reverse(BetweenZeroAndN(n, e)) =>
        BetweenZeroAndNF(n, Coattr.pure(Reverse(e)))
      case x =>
        P.coalgebra
          .apply(x)
          .map(Coattr.pure)
    }

  def dagAlgebra[T](implicit
      basis: Basis[DAG, T]
  ): CVAlgebra[DAG, T] =
    CVAlgebra {
      case Join(
            ll :< Join(_, _),
            _ :< Path(_, per, or, gr, rev)
          ) =>
        import com.gsk.kg.engine.optics._

        val updater = _joinR
          .composeLens(Join.r)
          .composePrism(_pathR)
          .composeLens(Path.o)

        val rndVar    = generateRndVariable()
        val updatedLL = updater.set(rndVar)(ll)

        joinR(updatedLL, pathR(rndVar, per, or, gr, rev))
      case Join(
            _ :< Path(sl, pel, _, gl, revl),
            _ :< Path(_, per, or, gr, revr)
          ) =>
        val rndVar = generateRndVariable()
        joinR(
          pathR(sl, pel, rndVar, gl, revl),
          pathR(rndVar, per, or, gr, revr)
        )
      case t =>
        t.map(toRecursive(_)).embed
    }

  def pathCoalgebra(
      s: StringVal,
      o: StringVal,
      g: List[StringVal]
  ): CoalgebraM[Option, DAG, PropertyExpression] =
    CoalgebraM[Option, DAG, PropertyExpression] {
      case Reverse(Uri(uri)) =>
        Some(Path(s, Uri(uri), o, g, true))
      case Uri(uri) =>
        Some(Path(s, Uri(uri), o, g, false))
      case SeqExpression(pel, per) =>
        Some(Join(pel, per))
      case Alternative(pel, per) =>
        Some(Union(pel, per))
      case _ =>
        None
    }

  def apply[T](implicit
      T: Basis[DAG, T]
  ): T => T = { t =>
    T.coalgebra(t).rewrite { case Path(s, pe, o, g, rev) =>
      val peFutu   = scheme.zoo.futu(reverseCoalgebra)
      val peAna    = scheme.anaM(pathCoalgebra(s, o, g))
      val dagHisto = scheme.zoo.histo(dagAlgebra)

      val reversedPushedDown = peFutu(pe)
      val unfoldedPaths =
        peAna(reversedPushedDown)
          .getOrElse(pathR(s, reversedPushedDown, o, g, rev))
      val internalVarReplace = dagHisto(unfoldedPaths)

      T.coalgebra(internalVarReplace)
    }
  }

  def phase[T](implicit T: Basis[DAG, T]): Phase[T, T] = Phase { t =>
    val result = apply(T)(t)
    (result != t)
      .pure[M]
      .ifM(
        Log.debug(
          "Optimizer(ReversePathRewrite)",
          s"resulting query: ${result.toTree.drawTree}"
        ),
        ().pure[M]
      ) *>
      result.pure[M]
  }
}
