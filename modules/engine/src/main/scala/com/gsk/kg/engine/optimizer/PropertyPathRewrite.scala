package com.gsk.kg.engine.optimizer

import cats.implicits._
import com.gsk.kg.engine.DAG.{Project => _, _}
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.Log
import com.gsk.kg.engine.M
import com.gsk.kg.engine.Phase
import com.gsk.kg.sparqlparser.PropertyExpression.Alternative
import com.gsk.kg.sparqlparser.PropertyExpression.Reverse
import com.gsk.kg.sparqlparser.PropertyExpression.SeqExpression
import com.gsk.kg.sparqlparser.PropertyExpression
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import higherkindness.droste.{Algebra, Basis, Embed, GTransM, Gather, Project, TransM, scheme}
import com.gsk.kg.sparqlparser.PropertyExpression.fixedpoint._

object PropertyPathRewrite {

  def trans[T](
      s: StringVal,
      o: StringVal,
      g: List[StringVal]
  )(implicit
      T: Basis[DAG, T]
  ): GTransM[Option, PropertyExpressionF, DAG, PropertyExpression, T] = GTransM {
    pb: PropertyExpressionF[PropertyExpression] =>
      Embed[PropertyExpressionF, PropertyExpression].algebra(pb) match {
        case Alternative(Reverse(el), Reverse(er)) =>
          Some(Union(pathR(o, el, s, g), pathR(o, er, s, g)))
        case Alternative(Reverse(el), per) =>
          Some(Union(pathR(o, el, s, g), pathR(s, per, o, g)))
        case Alternative(pel, Reverse(er)) =>
          Some(Union(pathR(s, pel, o, g), pathR(o, er, s, g)))
        case Alternative(pel, per) =>
          Some(Union(pathR(s, pel, o, g), pathR(s, per, o, g)))
        case SeqExpression(Reverse(el), Reverse(er)) =>
          Some(Join(pathR(o, el, s, g), pathR(o, er, s, g)))
        case SeqExpression(Reverse(el), per) =>
          Some(Join(pathR(o, el, s, g), pathR(s, per, o, g)))
        case SeqExpression(pel, Reverse(er)) =>
          Some(Join(pathR(s, pel, o, g), pathR(o, er, s, g)))
        case SeqExpression(pel, per) =>
          Some(Join(pathR(s, pel, o, g), pathR(s, per, o, g)))
        case Reverse(SeqExpression(pel, per)) =>
          Some(Join(pathR(o, pel, s, g), pathR(o, per, s, g)))
        case Reverse(Alternative(pel, per)) =>
          Some(Union(pathR(o, pel, s, g), pathR(o, per, s, g)))
        case Reverse(e) =>
          Some(Path(o, e, s, g))
        case _ => None
      }
  }

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    def generateRndVariable: VARIABLE = VARIABLE(
      s"?_${java.util.UUID.randomUUID().toString}"
    )

    T.coalgebra(t).rewrite { case x @ Path(s, pe, o, g) =>
      val gather: Gather[PropertyExpressionF, PropertyExpression, T] = (a, b) => Embed[PropertyExpressionF, PropertyExpression].algebra(b)
      val rec =
        scheme.gcataM[Option, PropertyExpressionF, PropertyExpression, PropertyExpression, T](
          trans[T](s, o, g)
            .algebra
            .gather(gather)
        )

      rec(pe) match {
        case Some(t) => T.coalgebra(t)
        case None    => x
      }

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
