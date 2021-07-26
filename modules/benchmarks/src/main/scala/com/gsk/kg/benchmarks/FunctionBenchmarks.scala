package com.gsk.kg.benchmarks

import java.util.concurrent.TimeUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.openjdk.jmh.annotations._
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.engine.rdf.TypedFuncs
import com.gsk.kg.engine.rdf.Typer
import com.gsk.kg.engine.rdf.RdfType
import org.apache.spark.sql.DataFrame
import com.gsk.kg.engine.functions.FuncArithmetics

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
class FunctionBenchmarks {

  val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("TestBenchmark")
    .getOrCreate();

  import spark.implicits._

  def isEven(n: Int) = n % 2 == 0

  val df = (1 to 1000).map(x => if (isEven(x)) 0 else 1).toDF("col")

  val numericDf = (1 to 1000).zip((1 to 1000).map(_ / 10)).toDF("col1", "col2")

  @Benchmark def ifUntyped(): Unit        = Impl.untypedIf(df)
  @Benchmark def ifTyped(): Unit          = Impl.typedIf(df)

  @Benchmark def addUntyped(): Unit       = Impl.untypedAdd(numericDf)
  @Benchmark def addTyped(): Unit         = Impl.typedAdd(numericDf)

  @Benchmark def substractUntyped(): Unit = Impl.untypedSubstract(numericDf)
  @Benchmark def substractTyped(): Unit   = Impl.typedSubstract(numericDf)

  @Benchmark def multiplyUntyped(): Unit  = Impl.untypedMultiply(numericDf)
  @Benchmark def multiplyTyped(): Unit    = Impl.typedMultiply(numericDf)

  @Benchmark def divideUntyped(): Unit    = Impl.untypedDivide(numericDf)
  @Benchmark def divideTyped(): Unit      = Impl.typedDivide(numericDf)

}

object Impl {

  def untypedIf(df: DataFrame): Unit = {
    df.withColumn(
      "result",
      FuncForms.`if`(
        df("col"),
        lit("under 100"),
        lit("over 100")
      )
    ).collect
  }

  def typedIf(df: DataFrame): Unit = {
    val typed = Typer.to(df)

    Typer
      .rename(
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
  }

  def untypedAdd(df: DataFrame) =
    df.withColumn(
      "result",
      FuncArithmetics.add(df("col1"), df("col2"))
    ).collect

  def untypedSubstract(df: DataFrame) =
    df.withColumn(
      "result",
      FuncArithmetics.subtract(df("col1"), df("col2"))
    ).collect

  def untypedMultiply(df: DataFrame) =
    df.withColumn(
      "result",
      FuncArithmetics.multiply(df("col1"), df("col2"))
    ).collect

  def untypedDivide(df: DataFrame) =
    df.withColumn(
      "result",
      FuncArithmetics.divide(df("col1"), df("col2"))
    ).collect

  def typedAdd(df: DataFrame) =
    Typer
      .rename(
        Typer
          .to(df)
          .withColumn(
            "result",
            TypedFuncs.add(col("col1"), col("col2"))
          )
      )
      .collect

  def typedSubstract(df: DataFrame) =
    Typer
      .rename(
        Typer
          .to(df)
          .withColumn(
            "result",
            TypedFuncs.subtract(col("col1"), col("col2"))
          )
      )
      .collect

  def typedMultiply(df: DataFrame) =
    Typer
      .rename(
        Typer
          .to(df)
          .withColumn(
            "result",
            TypedFuncs.multiply(col("col1"), col("col2"))
          )
      )
      .collect

  def typedDivide(df: DataFrame) =
    Typer
      .rename(
        Typer
          .to(df)
          .withColumn(
            "result",
            TypedFuncs.divide(col("col1"), col("col2"))
          )
      )
      .collect

}
