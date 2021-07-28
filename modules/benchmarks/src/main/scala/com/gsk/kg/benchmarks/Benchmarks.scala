package com.gsk.kg.benchmarks

import cats.data.NonEmptyList

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.gsk.kg.config.Config
import com.gsk.kg.engine.functions.FuncArithmetics
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.engine.rdf.RdfType
import com.gsk.kg.engine.rdf.TypedFuncs
import com.gsk.kg.engine.rdf.Typer

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class Benchmarks {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", "Benchmark")
    .set("spark.driver.host", "localhost")
    .set("spark.sql.codegen.wholeStage", "false")
    .set("spark.sql.shuffle.partitions", "1")

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .config(sparkConf)
    .getOrCreate()

  import spark.implicits._

  implicit val sc = spark.sqlContext

  def isEven(n: Int) = n % 2 == 0

  val df        = (1 to 1000).map(x => if (isEven(x)) 0 else 1).toDF("col")
  val numericDf = (1 to 1000).zip((1 to 1000).map(_ / 10)).toDF("col1", "col2")
  val spoDf = List(
    (
      "<http://example.org/alice>",
      "<http://xmlns.com/foaf/0.1/name>",
      "Alice"
    ),
    (
      "<http://example.org/alice>",
      "<http://xmlns.com/foaf/0.1/age>",
      "21"
    ),
    (
      "<http://example.org/bob>",
      "<http://xmlns.com/foaf/0.1/name>",
      "Bob"
    ),
    (
      "<http://example.org/bob>",
      "<http://xmlns.com/foaf/0.1/age>",
      "15"
    )
  ).toDF("s", "p", "o")

  val otherSpoDf = List(
    ("<http://asdf.com/pepe>", "<http://asdf.com/a>", "a"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/b>", "b"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/c>", "c"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/d>", "d"),
    ("<http://asdf.com/pepe>", "<http://asdf.com/e>", "e")
  ).toDF("s", "p", "o")

  @Benchmark def selectQueryUntyped(blackhole: Blackhole): Unit =
    Impl.selectQueryUntyped(otherSpoDf, blackhole)
  @Benchmark def selectQueryTyped(blackhole: Blackhole): Unit =
    Impl.selectQueryTyped(otherSpoDf, blackhole)
  @Benchmark def selectQueryWithFunctionsUntyped(blackhole: Blackhole): Unit =
    Impl.selectQueryWithFunctionsUntyped(spoDf, blackhole)
  @Benchmark def selectQueryWithFunctionsTyped(blackhole: Blackhole): Unit =
    Impl.selectQueryWithFunctionsTyped(spoDf, blackhole)
  @Benchmark def ifQueryUntyped(blackhole: Blackhole): Unit =
    Impl.ifQueryUntyped(spoDf, blackhole)
  @Benchmark def ifQueryTyped(blackhole: Blackhole): Unit =
    Impl.ifQueryTyped(spoDf, blackhole)
  @Benchmark def ifUntyped(blackhole: Blackhole): Unit =
    Impl.untypedIf(df, blackhole)
  @Benchmark def ifTyped(blackhole: Blackhole): Unit =
    Impl.typedIf(df, blackhole)
  @Benchmark def addUntyped(blackhole: Blackhole): Unit =
    Impl.untypedAdd(numericDf, blackhole)
  @Benchmark def addTyped(blackhole: Blackhole): Unit =
    Impl.typedAdd(numericDf, blackhole)
  @Benchmark def substractUntyped(blackhole: Blackhole): Unit =
    Impl.untypedSubstract(numericDf, blackhole)
  @Benchmark def substractTyped(blackhole: Blackhole): Unit =
    Impl.typedSubstract(numericDf, blackhole)
  @Benchmark def multiplyUntyped(blackhole: Blackhole): Unit =
    Impl.untypedMultiply(numericDf, blackhole)
  @Benchmark def multiplyTyped(blackhole: Blackhole): Unit =
    Impl.typedMultiply(numericDf, blackhole)
  @Benchmark def divideUntyped(blackhole: Blackhole): Unit =
    Impl.untypedDivide(numericDf, blackhole)
  @Benchmark def divideTyped(blackhole: Blackhole): Unit =
    Impl.typedDivide(numericDf, blackhole)
}

object Impl {

  def untypedIf(df: DataFrame, blackhole: Blackhole): Unit = {
    val result = df
      .withColumn(
        "result",
        FuncForms.`if`(
          df("col"),
          lit("under 100"),
          lit("over 100")
        )
      )
      .collect

    blackhole.consume(result)
  }

  def typedIf(df: DataFrame, blackhole: Blackhole): Unit = {
    val typed = Typer.`type`(df)

    val result = Typer
      .untype(
        typed
          .withColumn(
            "result",
            TypedFuncs.`if`(
              col("col"),
              ifTrue =
                Typer.createRecord(lit("under 100"), RdfType.String.repr),
              ifFalse = Typer.createRecord(lit("over 100"), RdfType.String.repr)
            )
          )
      )
      .collect

    blackhole.consume(result)
  }

  def untypedAdd(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.add(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def untypedSubstract(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.subtract(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def untypedMultiply(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.multiply(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def untypedDivide(df: DataFrame, blackhole: Blackhole) = {
    val result = df
      .withColumn(
        "result",
        FuncArithmetics.divide(df("col1"), df("col2"))
      )
      .collect
    blackhole.consume(result)
  }

  def typedAdd(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.add(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def typedSubstract(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.subtract(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def typedMultiply(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.multiply(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def typedDivide(df: DataFrame, blackhole: Blackhole) = {
    val result = Typer
      .untype(
        Typer
          .`type`(df)
          .withColumn(
            "result",
            TypedFuncs.divide(col("col1"), col("col2"))
          )
      )
      .collect
    blackhole.consume(result)
  }

  def ifQueryUntyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val query =
      """
          |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          |
          |SELECT ?name ?cat
          |WHERE {
          | ?s foaf:name ?name .
          | ?s foaf:age ?age .
          | BIND(IF(?age < 18, "child", "adult") AS ?cat)
          |}
          |""".stripMargin

    val result = com.gsk.kg.engine.Compiler
      .compile(df, query, Config.default)
      .right
      .get
      .collect

    blackhole.consume(result)
  }

  def ifQueryTyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val typed = Typer.`type`(df)

    val first = typed
      .filter(typed("p")("value") === "http://xmlns.com/foaf/0.1/name")
      .select(col("s").as("?s"), col("o").as("?name"))

    val second = typed
      .filter(typed("p")("value") === "http://xmlns.com/foaf/0.1/age")
      .select(col("s").as("?s"), col("o").as("?age"))

    val joined =
      first.join(second, "?s")

    val withNewColumn =
      joined.withColumn(
        "?cat",
        TypedFuncs.`if`(
          Typer.createRecord(col("?age")("value") < 18, RdfType.Boolean.repr),
          ifTrue = Typer.createRecord(lit("child"), RdfType.String.repr),
          ifFalse = Typer.createRecord(lit("adult"), RdfType.String.repr)
        )
      )

    val result = Typer
      .untype(withNewColumn)
      .select("?name", "?cat")
      .collect

    blackhole.consume(result)
  }

  def selectQueryWithFunctionsUntyped(df: DataFrame, blackhole: Blackhole)(
      implicit sc: SQLContext
  ) = {
    val query = """
      PREFIX dm: <http://asdf.com/>

      SELECT DISTINCT ?b ?a WHERE {
        ?s dm:q ?c;
           dm:w ?b;
           dm:e ?d;
           dm:r "asdf";
           dm:t "qwer" .
        BIND(URI(CONCAT("http://example.com/",?d)) as ?e) .
        BIND(URI(CONCAT("http://example.com?asdf=",?c)) as ?a) .
      }
      """

    val result = com.gsk.kg.engine.Compiler
      .compile(df, query, Config.default)
      .right
      .get
      .collect

    blackhole.consume(result)
  }

  def selectQueryWithFunctionsTyped(df: DataFrame, blackhole: Blackhole)(
      implicit sc: SQLContext
  ) = {
    val typed = Typer.`type`(df)

    val first = typed
      .filter(col("s")("value") === "http://asdf.com/q")
      .select(col("s").as("?s"), col("o").as("?c"))

    val second = typed
      .filter(col("s")("value") === "http://asdf.com/w")
      .select(col("s").as("?s"), col("o").as("?b"))

    val third = typed
      .filter(col("s")("value") === "http://asdf.com/e")
      .select(col("s").as("?s"), col("o").as("?d"))

    val fourth = typed
      .filter(
        col("s")("value") === "http://asdf.com/r" && col("o")(
          "value"
        ) === "asdf"
      )
      .select(col("s").as("?s"))

    val fifth = typed
      .filter(
        col("s")("value") === "http://asdf.com/t" && col("o")(
          "value"
        ) === "asdf"
      )
      .select(col("s").as("?s"))

    val joined =
      first
        .join(second, "?s")
        .join(third, "?s")
        .join(fourth, "?s")
        .join(fifth, "?s")

    val withNewBindings =
      joined
        .withColumn(
          "?e",
          TypedFuncs.uri(
            TypedFuncs.concat(
              Typer.createRecord(lit("http://example.com"), RdfType.Uri.repr),
              NonEmptyList.of(col("?d"))
            )
          )
        )
        .withColumn(
          "?e",
          TypedFuncs.uri(
            TypedFuncs.concat(
              Typer.createRecord(
                lit("http://example.com?asdf="),
                RdfType.Uri.repr
              ),
              NonEmptyList.of(col("?c"))
            )
          )
        )
        .distinct()

    val result = withNewBindings.collect()

    blackhole.consume(result)
  }

  def selectQueryUntyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val query = """
      PREFIX dm: <http://asdf.com/>

      SELECT * WHERE {
        ?s dm:a ?a;
           dm:b ?b;
           dm:c ?c;
           dm:d ?d;
           dm:e ?e .
      }
      """

    val result = com.gsk.kg.engine.Compiler
      .compile(df, query, Config.default)
      .right
      .get
      .collect

    assert(!result.isEmpty)

    blackhole.consume(result)
  }

  def selectQueryTyped(df: DataFrame, blackhole: Blackhole)(implicit
      sc: SQLContext
  ) = {
    val typed = Typer.`type`(df)

    val first = typed
      .filter(col("s")("value") === "http://asdf.com/a")
      .select(col("s").as("?s"), col("o").as("?a"))

    val second = typed
      .filter(col("s")("value") === "http://asdf.com/b")
      .select(col("s").as("?s"), col("o").as("?b"))

    val third = typed
      .filter(col("s")("value") === "http://asdf.com/c")
      .select(col("s").as("?s"), col("o").as("?c"))

    val fourth = typed
      .filter(col("s")("value") === "http://asdf.com/d")
      .select(col("s").as("?s"), col("o").as("?d"))

    val fifth = typed
      .filter(col("s")("value") === "http://asdf.com/e")
      .select(col("s").as("?s"), col("o").as("?e"))

    val joined = first
      .join(second, "?s")
      .join(third, "?s")
      .join(fourth, "?s")
      .join(fifth, "?s")

    val result = joined.collect()

    assert(!result.isEmpty)

    blackhole.consume(result)
  }
}
