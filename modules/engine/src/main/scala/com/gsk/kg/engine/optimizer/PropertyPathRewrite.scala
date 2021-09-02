package com.gsk.kg.engine.optimizer

import cats.Functor
import cats.data.Tuple2K
import cats.implicits._
import com.gsk.kg.engine.DAG.{Project => _, _}
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.Log
import com.gsk.kg.engine.M
import com.gsk.kg.engine.Phase
import com.gsk.kg.sparqlparser.PropertyExpression._
import com.gsk.kg.sparqlparser.PropertyExpression
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.PropertyExpression.fixedpoint._
import higherkindness.droste._
import higherkindness.droste.data._
import higherkindness.droste.syntax.all._
import higherkindness.droste.prelude._

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

  def dagAlgebra[T](implicit basis: Basis[DAG, T]): CVAlgebra[DAG, T] =
    CVAlgebra {
      case Join(
      ll :< Join(_, _ :< Path(slr, pelr, _, glr)),
      _ :< Path(_, per, or, gr)
      ) =>
        import com.gsk.kg.engine.optics._

        val updater = _joinR
          .composeLens(Join.r)
          .composePrism(_pathR)
          .composeLens(Path.o)

        val rndVar = generateRndVariable()
        val updatedLL = updater.modify(_ => rndVar)(ll)
        val r = pathR(rndVar, per, or, gr)
        val res = joinR(updatedLL, r)
        res
      //      case Join(
      //            _ :< Join(ll :< Join(lll, rrr), _ :< Path(slr, pelr, _, glr)),
      //            _ :< Path(_, per, or, gr)
      //          ) =>
      //        val rndVar = generateRndVariable
      //        val lr     = pathR(slr, pelr, rndVar, glr)
      //        val r      = pathR(rndVar, per, or, gr)
      //        val res    = joinR(joinR(ll, lr), r)
      //        res
      case Join(l :< Path(sl, pel, _, gl), r :< Path(_, per, or, gr)) =>
        val rndVar = generateRndVariable()
        joinR(pathR(sl, pel, rndVar, gl), pathR(rndVar, per, or, gr))

      case t => t.map(toRecursive(_)).embed
    }

  def peAlgebra[T](s: StringVal, o: StringVal, g: List[StringVal])(implicit
      basis: Basis[DAG, T]
  ): CVAlgebra[PropertyExpressionF, T] = {

    val alg: CVAlgebra[PropertyExpressionF, T] =
      CVAlgebra[PropertyExpressionF, T] {
//        case AlternativeF(
//              Wrap(_) :< ReverseF(fpel),
//              Wrap(_) :< ReverseF(fper)
//            ) =>
//          unionR(
//            pathR(o, toRecursive(fpel), s, g),
//            pathR(o, toRecursive(fper), s, g)
//          )
//        case AlternativeF(
//              Wrap(_) :< ReverseF(fpel),
//              Wrap(per) :< UriF(_)
//            ) =>
//          unionR(
//            pathR(o, toRecursive(fpel), s, g),
//            pathR(s, per, o, g)
//          )
//        case AlternativeF(
//              Wrap(pel) :< UriF(_),
//              Wrap(_) :< ReverseF(fper)
//            ) =>
//          unionR(
//            pathR(s, pel, o, g),
//            pathR(o, toRecursive(fper), s, g)
//          )
//        case AlternativeF(
//              Wrap(pel) :< UriF(_),
//              Wrap(per) :< UriF(_)
//            ) =>
//          unionR(
//            pathR(s, pel, o, g),
//            pathR(s, per, o, g)
//          )
//        case AlternativeF(dagL :< _, per) =>
//          val a = toRecursive(per)
//          unionR(
//            dagL,
//            pathR(s, a, o, g)
//          )

        case SeqExpressionF(
              Wrap(_) :< ReverseF(fpel),
              Wrap(_) :< ReverseF(fper)
            ) =>
//          val rndVar = generateRndVariable
          joinR(
            pathR(o, toRecursive(fpel), s, g),
            pathR(o, toRecursive(fper), s, g)
          )
        case SeqExpressionF(
              Wrap(_) :< ReverseF(fpel),
              Wrap(per) :< UriF(_)
            ) =>
//          val rndVar = generateRndVariable
          joinR(
            pathR(o, toRecursive(fpel), s, g),
            pathR(s, per, o, g)
          )
        case SeqExpressionF(
              Wrap(pel) :< UriF(_),
              Wrap(_) :< ReverseF(fper)
            ) =>
//          val rndVar = generateRndVariable
          joinR(
            pathR(s, pel, o, g),
            pathR(o, toRecursive(fper), s, g)
          )
        case SeqExpressionF(
              Wrap(pel) :< UriF(_),
              Wrap(per) :< UriF(_)
            ) =>
//          val rndVar = generateRndVariable
          joinR(
            pathR(s, pel, o, g),
            pathR(s, per, o, g)
          )
        case SeqExpressionF(dagL :< _, per) =>
          joinR(
            dagL,
            pathR(s, toRecursive(per), o, g)
          )

        //          case ReverseF(_ :< AlternativeF(pel, per)) =>
        //            wrapR(
        //              Alternative(Reverse(toRecursive(pel)), Reverse(toRecursive(per)))
        //            )
        case ReverseF(Wrap(pe) :< _) =>
          wrapR(Reverse(pe))
        case ReverseF(Wrap(pe) :< UriF(_)) =>
          pathR(o, pe, s, g)
        case UriF(x) =>
          wrapR(Uri(x))
      }

    alg
  }

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    T.coalgebra(t).rewrite { case Path(s, pe, o, g) =>
      val peHisto  = scheme.zoo.histo(peAlgebra(s, o, g))
      val dagHisto = scheme.zoo.histo(dagAlgebra)
      T.coalgebra(dagHisto(peHisto(pe)))
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
