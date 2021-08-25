package com.gsk.kg.engine.properties

import cats.Foldable
import cats.implicits.catsStdInstancesForList
import cats.implicits.catsSyntaxEitherId
import cats.syntax.either._

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.PropertyExpressionF.ColOrDf
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.engine.functions.PathFrame._
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Result

object FuncProperty {

  def alternative(
      df: DataFrame,
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {
    val col = df("p")

    (pel, per) match {
      case (Left(l), Left(r)) =>
        Left(
          when(
            col.startsWith("\"") && col.endsWith("\""),
            FuncForms.equals(trim(col, "\""), l) ||
              FuncForms.equals(trim(col, "\""), r)
          ).otherwise(
            FuncForms.equals(col, l) ||
              FuncForms.equals(col, r)
          )
        ).asRight
      case _ =>
        EngineError
          .InvalidPropertyPathArguments(
            s"Invalid arguments on property path: seq, pel: ${pel.toString}, per: ${per.toString}," +
              s" both should be of type column"
          )
          .asLeft
    }
  }

  def seq(
      df: DataFrame,
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {

    val resultL: Result[DataFrame] = (pel match {
      case Left(col)    => df.filter(df("p") === col)
      case Right(accDf) => accDf
    })
      .withColumnRenamed("s", "sl")
      .withColumnRenamed("p", "pl")
      .withColumnRenamed("o", "ol")
      .asRight[EngineError]

    val resultR: Result[DataFrame] = (per match {
      case Left(col) =>
        df.filter(df("p") === col)
      case Right(df) =>
        df
    }).withColumnRenamed("s", "sr")
      .withColumnRenamed("p", "pr")
      .withColumnRenamed("o", "or")
      .asRight[EngineError]

    for {
      l <- resultL
      r <- resultR
    } yield {
      Right(
        l
          .join(
            r,
            l("ol") <=> r("sr"),
            "inner"
          )
          .select(l("sl"), r("or"))
          .withColumnRenamed("sl", "s")
          .withColumnRenamed("or", "o")
      )
    }
  }

  def betweenNAndM(
      df: DataFrame,
      maybeN: Option[Int],
      maybeM: Option[Int],
      e: ColOrDf
  )(implicit
      sc: SQLContext
  ): Result[ColOrDf] = {

    val checkArgs: Either[EngineError, (Int, Int)] = (maybeN, maybeM) match {
      case (Some(n), Some(m)) if n < 0 || m < 0 =>
        EngineError
          .InvalidPropertyPathArguments(
            "Arguments n and m can't be less than zero"
          )
          .asLeft
      case (Some(n), Some(m)) if n >= m =>
        (n, n).asRight // N >= M
      case (Some(n), Some(m)) =>
        (n, m).asRight // N < M
      case (Some(n), None) =>
        // TODO: Should be set a hard limit by configuration to avoid cycles???
        (n, 100).asRight // N or more
      case (None, Some(m)) =>
        (0, m).asRight // Between 0 and M
      case (None, None) =>
        EngineError
          .InvalidPropertyPathArguments(
            "Arguments n and m can't be both none"
          )
          .asLeft
    }

    checkArgs.flatMap { case (effectiveN, effectiveM) =>
      val onePaths = getOneLengthPaths(e match {
        case Right(predDf) => predDf
        case Left(predCol) => df.filter(predCol)
      })

      val (maxLength, pathsFrame) =
        constructPathFrame(onePaths, limit = Some(effectiveM))

      val effectiveRange =
        effectiveN to (if (effectiveM > maxLength) maxLength else effectiveM)

      val paths =
        effectiveRange
          .map(i => getNLengthPathTriples(df, pathsFrame, i))
          .toList

      val betweenNAndMPaths = Foldable[List].fold(paths)

      toSPOG(betweenNAndMPaths)
        .asRight[Column]
        .asRight[EngineError]
    }
  }

  def notOneOf(df: DataFrame, es: List[ColOrDf]): Result[ColOrDf] = {

    val resolveColCnd: ColOrDf => Result[Column] = { expr: ColOrDf =>
      expr match {
        case Right(_) =>
          EngineError
            .InvalidPropertyPathArguments(
              "Dataframe not allowed as argument for NotOneOf"
            )
            .asLeft
        case Left(predCol) =>
          not(predCol).asRight
      }
    }

    val zero = lit(true).asRight[EngineError]

    es.foldLeft(zero) { case (accOrError, colOrDf) =>
      for {
        acc    <- accOrError
        colCnd <- resolveColCnd(colOrDf)
      } yield acc && colCnd
    }.map { cnd =>
      df.filter(cnd).asRight[Column]
    }
  }

  def uri(df: DataFrame, s: String): ColOrDf =
    (df("p") <=> lit(s)).asLeft

}
