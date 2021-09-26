package com.gsk.kg.engine.typed.functions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

import com.gsk.kg.engine.DataFrameTyper
import com.gsk.kg.engine.RdfType
import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators
import com.gsk.kg.engine.syntax._

import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class TypedFuncFormsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on Forms" when {

    "TypedFuncForms.equals" should {

      "operates on string correctly" in {

        val df = List(
          ("\"alice\"", "\"alice\"", "true"),
          ("\"alice\"", "\"alice\"", "true"),
          (
            "\"alice\"",
            "\"alice\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "true"
          ),
          ("\"alice\"", "\"alice\"", "true"),
          (
            "\"alice\"",
            "\"alice\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "true"
          ),
          (
            "\"alice\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "\"alice\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "true"
          )
        ).toTypedDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("1", "1", "true"),
            ("1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("1", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "true"),
            (
              "1.23",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "1.23",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "1.23",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val plainNumberWrongCases = List(
            ("1", "2", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("1", "\"0\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "false"),
            (
              "1.23",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "1.23",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "1.23",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result = df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.0",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val intWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"0\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (intCorrectCases ++ intWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result = df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.0",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val integerWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"0\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (integerCorrectCases ++ integerWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result = df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.0",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val decimalWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.23",
              "true"
            ),
            (
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val floatWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (floatCorrectCases ++ floatWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.23",
              "true"
            ),
            (
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val doubleWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.23",
              "true"
            ),
            (
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val numericWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (numericCorrectCases ++ numericWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.equals(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "operate on equal dates correctly" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime),
              toRDFDateTime(datetime),
              "true"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(TypedFuncForms.equals(df("a"), df("b")))
            .collect() shouldEqual df.select(df("expected")).collect()

        }
      }

      "operate on different dates correctly" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime.plusSeconds(1)),
              toRDFDateTime(datetime),
              "false"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(TypedFuncForms.equals(df("a"), df("b")))
            .collect() shouldEqual df.select(df("expected")).collect()
        }
      }
    }

    "TypedFuncForms.gt" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("2", "1", "true"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "true"),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val plainNumberWrongCases = List(
            ("1", "2", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "false"),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "true"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.0",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val intWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (intCorrectCases ++ intWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "true"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.0",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val integerWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (integerCorrectCases ++ integerWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "true"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.0",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val decimalWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.22",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val floatWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (floatCorrectCases ++ floatWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.22",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val doubleWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.22",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val numericWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (numericCorrectCases ++ numericWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          ("2", "1", "true")
        ).toTypedDF("a", "b", "expected")

        df.select(TypedFuncForms.gt(df("a"), df("b"))).collect() shouldEqual df
          .select(df("expected"))
          .collect()
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime.plusSeconds(1)),
              toRDFDateTime(datetime),
              "true"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(TypedFuncForms.gt(df("a"), df("b")))
            .collect() shouldEqual df.select(df("expected")).collect()
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
              "false"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(
            TypedFuncForms.gt(df("a"), df("b"))
          ).collect() shouldEqual df.select(df("expected")).collect()
        }
      }
    }

    "TypedFuncForms.lt" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("1", "2", "true"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "true"),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val plainNumberWrongCases = List(
            ("2", "1", "false"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "false"),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val intWrongCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "false"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (intCorrectCases ++ intWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val integerWrongCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "false"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (integerCorrectCases ++ integerWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "2", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.1",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val decimalWrongCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "false"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.1",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.24",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val floatWrongCases = List(
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.23",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (floatCorrectCases ++ floatWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.24",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val doubleWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.22",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.24",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val numericWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.22",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (numericCorrectCases ++ numericWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lt(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          (1, 2, "true")
        ).toTypedDF("a", "b", "expected")

        df.select(TypedFuncForms.lt(df("a"), df("b"))).collect() shouldEqual df
          .select(df("expected"))
          .collect()
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime),
              toRDFDateTime(datetime.plusSeconds(1)),
              "true"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(TypedFuncForms.lt(df("a"), df("b")))
            .collect() shouldEqual df.select(df("expected")).collect()
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
              "false"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(
            TypedFuncForms.lt(df("a"), df("b"))
          ).collect() shouldEqual df.select(df("expected")).collect()
        }
      }
    }

    "TypedFuncForms.gte" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("2", "1", "true"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "true"),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val plainNumberWrongCases = List(
            ("1", "2", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "false"),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "true"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.0",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val intWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (intCorrectCases ++ intWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "true"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.0",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val integerWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (integerCorrectCases ++ integerWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "true"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.0",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val decimalWrongCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "2", "false"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.1",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.22",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val floatWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (floatCorrectCases ++ floatWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.22",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val doubleWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.22",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val numericWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.24",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (numericCorrectCases ++ numericWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.gte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          (2, 1, "true"),
          (2, 2, "true")
        ).toTypedDF("a", "b", "expected")

        df.select(TypedFuncForms.gte(df("a"), df("b"))).collect() shouldEqual df
          .select(df("expected"))
          .collect()
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime.plusSeconds(1)),
              toRDFDateTime(datetime),
              "true"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(TypedFuncForms.gte(df("a"), df("b")))
            .collect() shouldEqual df.select(df("expected")).collect()
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
              "false"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(
            TypedFuncForms.gte(df("a"), df("b"))
          ).collect() shouldEqual df.select(df("expected")).collect()
        }
      }
    }

    "TypedFuncForms.lte" should {

      "operates on numbers correctly" when {

        "first argument is not typed number and second is other number type" in {
          val plainNumberCorrectCases = List(
            ("1", "2", "true"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
            ("1", "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "true"),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "1.23",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val plainNumberWrongCases = List(
            ("2", "1", "false"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
            ("2", "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "false"),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "1.24",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INT type number and second is other number type" in {

          val intCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val intWrongCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "false"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (intCorrectCases ++ intWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is INTEGER type number and second is other number type" in {

          val integerCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "2", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val integerWrongCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "false"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "1.1",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (integerCorrectCases ++ integerWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result   = df.select(TypedFuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DECIMAL type number and second is other number type" in {

          val decimalCorrectCases = List(
            ("\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "2", "true"),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.1",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val decimalWrongCases = List(
            ("\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1", "false"),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "1.1",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (decimalCorrectCases ++ decimalWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is FLOAT type number and second is other number type" in {

          val floatCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.24",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val floatWrongCases = List(
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "1.23",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (floatCorrectCases ++ floatWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is DOUBLE type number and second is other number type" in {

          val doubleCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.24",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val doubleWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "1.22",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (doubleCorrectCases ++ doubleWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }

        "first argument is NUMERIC type number and second is other number type" in {

          val numericCorrectCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.24",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "true"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.24\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "true"
            )
          )

          val numericWrongCases = List(
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "1.22",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#float>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#double>",
              "false"
            ),
            (
              "\"1.23\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "\"1.22\"^^<http://www.w3.org/2001/XMLSchema#numeric>",
              "false"
            )
          )

          val df = (numericCorrectCases ++ numericWrongCases).toTypedDF(
            "arg1",
            "arg2",
            "expected"
          )

          val result =
            df.select(TypedFuncForms.lte(df("arg1"), df("arg2")))
          val expected = df.select(col("expected"))

          result.collect() shouldEqual expected.collect()
        }
      }

      "work for integer values" in {

        val df = List(
          (1, 2, "true"),
          (2, 2, "true")
        ).toTypedDF("a", "b", "expected")

        df.select(TypedFuncForms.lte(df("a"), df("b"))).collect() shouldEqual df
          .select(df("expected"))
          .collect()
      }

      "work in datetimes without a zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(datetime),
              toRDFDateTime(datetime.plusSeconds(1)),
              "true"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(TypedFuncForms.lte(df("a"), df("b")))
            .collect() shouldEqual df.select(df("expected")).collect()
        }
      }

      "work in datetimes with zone" in {

        forAll { datetime: LocalDateTime =>
          val df = List(
            (
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
              toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
              "false"
            )
          ).toTypedDF("a", "b", "expected")

          df.select(
            TypedFuncForms.lte(df("a"), df("b"))
          ).collect() shouldEqual df.select(df("expected")).collect()
        }
      }
    }

    "TypedFuncForms.and" should {
      // TODO: Implement tests on and
    }

    "TypedFuncForms.or" should {
      // TODO: Implement tests on or
    }

    "TypedFuncForms.negate" should {

      "return the input boolean column negated" in {

        val df = List(
          ("true", "false"),
          ("false", "true")
        ).toTypedDF("boolean", "negated")

        val result = df.select(TypedFuncForms.negate(df("boolean"))).collect

        result shouldEqual df.select(df("negated")).collect()
      }

      "fail when the input column contain values that are not boolean values" ignore {

        val df = List(
          "\"a\""
        ).toTypedDF("boolean")

        val caught = intercept[AnalysisException] {
          df.select(TypedFuncForms.negate(df("boolean"))).collect
        }

        caught.getMessage should contain
        "cannot resolve '(NOT `boolean`)' due to data type mismatch"
      }
    }

    "TypedFuncForms.in" should {

      "return true when exists" in {

        val df = List(
          ("2", "1", "2", "3", "true")
        ).toTypedDF("origin", "e1", "e2", "e3", "expected")

        val result =
          df.select(
            TypedFuncForms.in(df("origin"), List(df("e1"), df("e2"), df("e3")))
          ).collect

        result shouldEqual df.select("expected").collect()
      }

      "return false when empty" in {
        val df = List(("\"hello\"", "false")).toTypedDF("e1", "expected")

        val result =
          df.select(TypedFuncForms.in(df("e1"), List.empty[Column])).collect

        result shouldEqual df.select("expected").collect
      }

      "return true when exists with mixed types" ignore {

        val df = List(
          ("2", "<http://example/iri>", "\"str\"", "2.0", "true")
        ).toTypedDF("origin", "e1", "e2", "e3", "expected")

        val result =
          df.select(
            TypedFuncForms.in(df("origin"), List(df("e1"), df("e2"), df("e3")))
          ).collect

        result shouldEqual df.select("expected").collect()
      }

      "return true when exists and there are null expressions" in {

        val df = List(
          ("2", null, "2", "true")
        ).toTypedDF("origin", "e1", "e2", "expected")

        val result =
          df.select(TypedFuncForms.in(df("origin"), List(df("e1"), df("e2"))))
            .collect

        result shouldEqual df.select("expected").collect
      }

      "return true when exists and there are null expressions 2" in {

        val df = List(
          ("2", "2", null, "true")
        ).toTypedDF("origin", "e1", "e2", "expected")

        val result =
          df.select(TypedFuncForms.in(df("origin"), List(df("e1"), df("e2"))))
            .collect

        result shouldEqual df.select("expected").collect()
      }
    }

    "TypedFuncForms.sameTerm" should {

      "return expected results" in {

        val df = List(
          ("\"hello\"@en", "\"hello\"@en", "true"),
          ("\"hello\"@en", "\"hello\"", "true"),
          ("\"hello\"@en", "\"hello\"@es", "false"),
          ("\"hello\"@en", "\"hi\"@en", "false"),
          (
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "true"
          ),
          ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1", "true"),
          (
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "false"
          )
        ).toTypedDF("expr1", "expr2", "expected")

        val result =
          df.select(TypedFuncForms.sameTerm(df("expr1"), df("expr2"))).collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "TypedFuncForms.if" should {

      "return expected results with plain booleans" in {

        val df = List(
          ("true", "\"yes\"", "\"no\"", "\"yes\""),
          ("false", "\"yes\"", "\"no\"", "\"no\"")
        ).toTypedDF("cnd", "ifTrue", "ifFalse", "expected")

        val result =
          df.select(TypedFuncForms.`if`(df("cnd"), df("ifTrue"), df("ifFalse")))
            .collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }

      "return if true when other types evaluate to true" in {

        val df = List(
          (
            "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            "\"yes\"",
            "\"no\"",
            "\"yes\""
          ),
          (
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"yes\"",
            "\"no\"",
            "\"yes\""
          ),
          ("true", "\"yes\"", "\"no\"", "\"yes\""),
          (
            "\"abc\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "\"yes\"",
            "\"no\"",
            "\"yes\""
          ),
          ("\"abc\"", "\"yes\"", "\"no\"", "\"yes\""),
          (
            "\"1.2\"^^<http://www.w3.org/2001/XMLSchema#double>",
            "\"yes\"",
            "\"no\"",
            "\"yes\""
          ),
          ("1.2", "\"yes\"", "\"no\"", "\"yes\"")
        ).toTypedDF("cnd", "ifTrue", "ifFalse", "expected")

        val result =
          df.select(TypedFuncForms.`if`(df("cnd"), df("ifTrue"), df("ifFalse")))
            .collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }

      "return if false when other types evaluate to false" in {

        val df = List(
          (
            "\"abc\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            "\"yes\"",
            "\"no\"",
            "\"no\""
          ),
          (
            "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            "\"yes\"",
            "\"no\"",
            "\"no\""
          ),
          (
            "\"abc\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"yes\"",
            "\"no\"",
            "\"no\""
          ),
          ("false", "\"yes\"", "\"no\"", "\"no\""),
          (
            "\"\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "\"yes\"",
            "\"no\"",
            "\"no\""
          ),
          ("\"\"", "\"yes\"", "\"no\"", "\"no\""),
          (
            "\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"yes\"",
            "\"no\"",
            "\"no\""
          ),
          ("0", "\"yes\"", "\"no\"", "\"no\""),
          ("NaN", "\"yes\"", "\"no\"", "\"no\""),
          (
            "\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>",
            "\"yes\"",
            "\"no\"",
            "\"no\""
          )
        ).toTypedDF("cnd", "ifTrue", "ifFalse", "expected")

        val result =
          df.select(TypedFuncForms.`if`(df("cnd"), df("ifTrue"), df("ifFalse")))
            .collect
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "TypedFuncForms.effectiveBooleanValue" should {

      "return expected values" in {
        val df = List(
          ("\"abc\"^^<http://www.w3.org/2001/XMLSchema#boolean>", "false"),
          ("\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>", "false"),
          ("\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>", "true"),
          ("\"abc\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
          ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "true"),
          ("true", "true"),
          ("false", "false"),
          ("\"\"^^<http://www.w3.org/2001/XMLSchema#string>", "false"),
          ("\"\"", "false"),
          ("\"abc\"^^<http://www.w3.org/2001/XMLSchema#string>", "true"),
          ("\"abc\"", "true"),
          ("\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>", "false"),
          ("0", "false"),
          ("NaN", "false"),
          ("\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>", "false"),
          ("\"1.2\"^^<http://www.w3.org/2001/XMLSchema#double>", "true"),
          ("1.2", "true")
        ).toTypedDF("elem", "expected")

        val result =
          df.select(TypedFuncForms.effectiveBooleanValue(df("elem"))).collect()
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "TypedFuncForms.bound" should {

      "return expected value" in {

        val df = List(
          ("\"abc\"^^<http://www.w3.org/2001/XMLSchema#boolean>", "true"),
          ("\"NaN\"", "true"),
          ("\"INF\"", "true"),
          (null, "false")
        ).toTypedDF("elem", "expected")

        val result =
          df.select(TypedFuncForms.bound(df("elem"))).collect()
        val expected = df.select(df("expected")).collect()

        result shouldEqual expected
      }
    }

    "TypedFuncForms.coalesce" should {

      "return expected value" when {

        "first elem bound and second null" in {
          val df = List(
            ("2", null, "2")
          ).toTypedDF("?x", "?y", "expected")

          val cols = List(df("?x"), DataFrameTyper.NullLiteral)

          val result   = df.select(TypedFuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "first elem null and second bound" in {
          val df = List(
            ("2", null, "2")
          ).toTypedDF("?x", "?y", "expected")

          val cols = List(DataFrameTyper.NullLiteral, df("?x"))

          val result   = df.select(TypedFuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "first elem literal and second bound" in {
          val df = List(
            ("2", null, "5")
          ).toTypedDF("?x", "?y", "expected")

          val cols = List(RdfType.Int(lit(5)), df("?x"))

          val result   = df.select(TypedFuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "first elem not bound and second literal" in {
          val df = List(
            ("2", null, "3")
          ).toTypedDF("?x", "?y", "expected")

          val cols = List(df("?y"), RdfType.Int(lit("3")))

          val result   = df.select(TypedFuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }

        "one elem not bound" in {
          val df = List(
            ("2", null, null)
          ).toTypedDF("?x", "?y", "expected")

          val cols = List(df("?y"))

          val result   = df.select(TypedFuncForms.coalesce(cols)).collect()
          val expected = df.select(df("expected")).collect()

          result shouldEqual expected
        }
      }
    }
  }

  def toRDFDateTime(datetime: TemporalAccessor): String =
    "\"" + DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
      .format(datetime) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
}
