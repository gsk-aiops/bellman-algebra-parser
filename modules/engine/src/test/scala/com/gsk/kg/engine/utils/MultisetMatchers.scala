package com.gsk.kg.engine.utils

import com.gsk.kg.engine.Multiset
import com.gsk.kg.engine.relational.Relational
import com.gsk.kg.engine.relational.Relational.ops._

import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers

trait MultisetMatchers extends Matchers {

  def equalsMultiset[A: Relational](right: Multiset[A]): Matcher[Multiset[A]] =
    equalsBindings(right) and equalsDataframe(right)

  def equalsBindings[A: Relational](right: Multiset[A]): Matcher[Multiset[A]] =
    new Matcher[Multiset[A]] {
      override def apply(left: Multiset[A]): MatchResult = MatchResult(
        left.bindings === right.bindings,
        "{0} bindings are different than {1}",
        "{0} bindings are equal as {1}",
        Vector(left.bindings, right.bindings),
        Vector(left.bindings, right.bindings)
      )
    }

  def equalsDataframe[A: Relational](right: Multiset[A]): Matcher[Multiset[A]] =
    new Matcher[Multiset[A]] {
      override def apply(left: Multiset[A]): MatchResult = {
        lazy val leftCollected  = left.relational.collect.map(_.toSeq.toSet)
        lazy val rightCollected = right.relational.collect.map(_.toSeq.toSet)
        MatchResult(
          leftCollected === rightCollected,
          "{0} dataframes are different {1}",
          "{0} dataframes are equal {1}",
          Vector(leftCollected, rightCollected),
          Vector(leftCollected, rightCollected)
        )
      }
    }
}
