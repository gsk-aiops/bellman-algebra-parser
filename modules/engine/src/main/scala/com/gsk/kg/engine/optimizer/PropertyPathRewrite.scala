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

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    T.coalgebra(t).rewrite { case x @ Path(s, pe, o, g) =>
      def toRecursive[F[_]: Functor, R: Embed[F, *]](attr: Attr[F, _]): R = {
        def fix[A](attr: Attr[F, _]): Fix[F] =
          Fix(Functor[F].map(Attr.un(attr)._2)(fix))

        val fixed: Fix[F] = fix(attr)

        scheme[Fix].gcata(Embed[F, R].algebra.gather(Gather.cata)).apply(fixed)
      }

      val alg: CVAlgebra[PropertyExpressionF, T] =
        CVAlgebra {
          case AlternativeF(Wrap(pel) :< UriF(x), Wrap(per) :< UriF(y)) =>
            unionR(
              pathR(s, pel, o, g),
              pathR(s, per, o, g)
            )

          case AlternativeF(dagL :< _, per) =>
            unionR(
              dagL,
              pathR(s, toRecursive(per), o, g)
            )

          case UriF(x) =>
            wrapR(Uri(x))
        }

      val fn = scheme.zoo.histo(alg)

      T.coalgebra(fn(pe))

//      PropertyExpressionRewrite.apply[DAG[T]](s, o, g, pe)
//      pe match {
//        case Alternative(Reverse(el), Reverse(er)) =>
//          Union(pathR(o, el, s, g), pathR(o, er, s, g))
//        case Alternative(Reverse(el), per) =>
//          Union(pathR(o, el, s, g), pathR(s, per, o, g))
//        case Alternative(pel, Reverse(er)) =>
//          Union(pathR(s, pel, o, g), pathR(o, er, s, g))
//        case Alternative(pel, per) =>
//          Union(pathR(s, pel, o, g), pathR(s, per, o, g))
//        case SeqExpression(Reverse(el), Reverse(er)) =>
//          val rndVar = generateRndVariable
//          Join(pathR(o, el, rndVar, g), pathR(rndVar, er, s, g))
//        case SeqExpression(Reverse(el), per) =>
//          val rndVar = generateRndVariable
//          Join(pathR(rndVar, el, s, g), pathR(rndVar, per, o, g))
//        case SeqExpression(pel, Reverse(er)) =>
//          val rndVar = generateRndVariable
//          Join(pathR(s, pel, rndVar, g), pathR(o, er, rndVar, g))
//        case SeqExpression(pel, per) =>
//          val rndVar = generateRndVariable
//          Join(pathR(s, pel, rndVar, g), pathR(rndVar, per, o, g))
//        case Reverse(SeqExpression(pel, per)) =>
//          val rndVar = generateRndVariable
//          Join(pathR(o, pel, rndVar, g), pathR(rndVar, per, s, g))
//        case Reverse(Alternative(pel, per)) =>
//          Union(pathR(o, pel, s, g), pathR(o, per, s, g))
//        case Reverse(e) =>
//          Path(o, e, s, g)
//        case _ =>
//          x
//      }
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
