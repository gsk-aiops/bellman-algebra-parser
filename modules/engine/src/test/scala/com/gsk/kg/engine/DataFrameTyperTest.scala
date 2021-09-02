package com.gsk.kg.engine

import com.gsk.kg.config.Config
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.all._
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class DataFrameTyperTest
  extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks {

  "DataFrameTyper" should {

    "type arbitrary dataframes" in {
      val config =
        Config.default.copy(typeDataframe = true, formatRdfOutput = true)

      forAll { df: DataFrame =>
        val typed = DataFrameTyper.typeDataFrame(df, config)
        val untyped = RdfFormatter.formatDataFrame(typed, config)

        df.collect shouldEqual untyped.collect
      }
    }
  }
}
