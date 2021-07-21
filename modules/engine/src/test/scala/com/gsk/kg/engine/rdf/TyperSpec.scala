package com.gsk.kg.engine.rdf

import com.gsk.kg.engine.compiler.SparkSpec

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.gsk.kg.engine.scalacheck.DataFrameArbitraries
import org.apache.spark.sql.DataFrame

class TyperSpec
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with DataFrameArbitraries
    with SparkSpec {

  "Typer" should "add all needed types to the dataframe" in {
    forAll { df: DataFrame =>
      val result = Typer.to(df)
      val back = Typer.from(result)

      df.collect shouldEqual back.collect
    }
  }
}
