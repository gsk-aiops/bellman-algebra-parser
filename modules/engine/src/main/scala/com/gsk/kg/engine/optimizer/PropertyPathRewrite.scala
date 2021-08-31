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
import higherkindness.droste._
import higherkindness.droste.data._
import com.gsk.kg.sparqlparser.PropertyExpression.fixedpoint._

object PropertyPathRewrite {

//      val result: DAG[T] =
//        Embed[PropertyExpressionF, PropertyExpression].algebra(pb) match {
////          case Alternative(Alternative(pell, pelr), per) =>
////            Union(
////              unionR(pathR(s, pell, o, g), pathR(s, pelr, o, g)),
////              pathR(s, per, o, g)
////            )
//          case Alternative(Reverse(el), Reverse(er)) =>
//            Union(pathR(o, el, s, g), pathR(o, er, s, g))
//          case Alternative(Reverse(el), per) =>
//            Union(pathR(o, el, s, g), pathR(s, per, o, g))
//          case Alternative(pel, Reverse(er)) =>
//            Union(pathR(s, pel, o, g), pathR(o, er, s, g))
//          case Alternative(pel, per) =>
//            Union(pathR(s, pel, o, g), pathR(s, per, o, g))
//          case SeqExpression(Reverse(el), Reverse(er)) =>
//            Join(pathR(o, el, s, g), pathR(o, er, s, g))
//          case SeqExpression(Reverse(el), per) =>
//            Join(pathR(o, el, s, g), pathR(s, per, o, g))
//          case SeqExpression(pel, Reverse(er)) =>
//            Join(pathR(s, pel, o, g), pathR(o, er, s, g))
//          case SeqExpression(pel, per) =>
//            Join(pathR(s, pel, o, g), pathR(s, per, o, g))
//          case Reverse(SeqExpression(pel, per)) =>
//            Join(pathR(o, pel, s, g), pathR(o, per, s, g))
//          case Reverse(Alternative(pel, per)) =>
//            Union(pathR(o, pel, s, g), pathR(o, per, s, g))
//          case Reverse(e) =>
//            Path(o, e, s, g)
//          case x => Wrap(x)
//        }
  //println(s"Output: $result")

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    def generateRndVariable: VARIABLE = VARIABLE(
      s"?_${java.util.UUID.randomUUID().toString}"
    )

    /*

    fn: Trans[PropertyExpression, PropertyExpression]

     t: T

     case Path(_, pe, _, _) => Union()

     result: T

     */

    T.coalgebra(t).rewrite { case x @ Path(s, pe, o, g) =>
      val alg: CVCoalgebra[DAG, PropertyExpression] = {
        CVCoalgebra[DAG, PropertyExpression] {
          case Alternative(Reverse(el), Reverse(er)) =>
            union(
              Coattr.roll(path(o, el, s, g)),
              Coattr.roll(path(o, er, s, g))
            )
          case Alternative(Alternative(pell, pelr), per) =>
            union(
              Coattr.roll(union(
                Coattr.roll(path(s, pell, o, g)),
                Coattr.roll(path(s, pelr, o, g))
              )),
              Coattr.roll(path(s, per, o, g))
            )
          case Alternative(pel, per) =>
            union(
              Coattr.roll(path(s, pel, o, g)),
              Coattr.roll(path(s, per, o, g))
            )
          case PropertyExpression.Uri(s) =>
            wrap(pe)
        }
      }

      val fn = scheme.zoo.futu(alg)

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
