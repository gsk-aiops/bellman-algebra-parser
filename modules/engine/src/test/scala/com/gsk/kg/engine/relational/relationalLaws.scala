package com.gsk.kg.engine.relational

import cats.Eq
import cats.laws._
import cats.kernel.laws.discipline._
import org.apache.spark.sql.SQLContext
import org.scalacheck.Arbitrary
import org.scalacheck.Prop
import org.typelevel.discipline.Laws

trait RelationalLaws[A] {
  implicit val R: Relational[A]

  def unionEmptyRight(df: A)(implicit sc: SQLContext): IsEq[A] =
    Relational[A].union(df, Relational[A].empty) <-> df

  def unionEmptyLeft(df: A)(implicit sc: SQLContext): IsEq[A] =
    Relational[A].union(Relational[A].empty, df) <-> df

}

object RelationalLaws {
  def apply[A](implicit A: Relational[A], Eq: Eq[A]): RelationalLaws[A] =
    new RelationalLaws[A] {
      val R: Relational[A] = A
    }
}

trait RelationalTests[A] extends Laws {
  def laws: RelationalLaws[A]

  def relational(implicit A: Arbitrary[A], eq: Eq[A], sc: SQLContext): RuleSet =
    new SimpleRuleSet(
      name = "Relational",
      "unionEmptyRight" -> Prop.forAll { (a: A) => laws.unionEmptyRight(a) },
      "unionEmptyLeft" -> Prop.forAll { (a: A) => laws.unionEmptyLeft(a) }
    )
}
