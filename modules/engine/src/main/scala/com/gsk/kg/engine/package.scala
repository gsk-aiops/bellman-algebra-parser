package com.gsk.kg

import cats.data.Chain
import cats.data.Kleisli
import cats.data.ReaderWriterStateT
import cats.instances.either._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import com.gsk.kg.config.Config
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.sparqlparser.Result
import higherkindness.droste.util.newtypes.@@
import org.slf4j.LoggerFactory

package object engine {

  sealed trait LogLevel
  object LogLevel {
    case object Debug   extends LogLevel
    case object Info    extends LogLevel
    case object Warning extends LogLevel
    case object Error   extends LogLevel
  }

  final case class LogMessage(
      level: LogLevel,
      phase: String,
      message: String
  )

  type Log = Chain[LogMessage]
  object Log {
    def debug(phase: String, message: String): M[Unit] =
      M.tell[Result, Config, Log, DataFrame @@ Untyped](
        Chain(LogMessage(LogLevel.Debug, phase, message))
      )
    def info(phase: String, message: String): M[Unit] =
      M.tell[Result, Config, Log, DataFrame @@ Untyped](
        Chain(LogMessage(LogLevel.Info, phase, message))
      )
    def warning(phase: String, message: String): M[Unit] =
      M.tell[Result, Config, Log, DataFrame @@ Untyped](
        Chain(LogMessage(LogLevel.Warning, phase, message))
      )
    def error(phase: String, message: String): M[Unit] =
      M.tell[Result, Config, Log, DataFrame @@ Untyped](
        Chain(LogMessage(LogLevel.Error, phase, message))
      )

    def run(log: Log): Unit = {
      def bellmanPhase(phase: String) = "Bellman " + phase
      log.map {
        case LogMessage(LogLevel.Debug, phase, message) =>
          val logger = LoggerFactory.getLogger(bellmanPhase(phase))
          logger.debug(message)
        case LogMessage(LogLevel.Info, phase, message) =>
          val logger = LoggerFactory.getLogger(bellmanPhase(phase))
          logger.info(message)
        case LogMessage(LogLevel.Warning, phase, message) =>
          val logger = LoggerFactory.getLogger(bellmanPhase(phase))
          logger.warn(message)
        case LogMessage(LogLevel.Error, phase, message) =>
          val logger = LoggerFactory.getLogger(bellmanPhase(phase))
          logger.error(message)
      }
      ()
    }
  }

  object SPOEncoder {
    implicit val spoEncoder: Encoder[Row] = RowEncoder(
      StructType(
        List(
          StructField("s", StringType),
          StructField("p", StringType),
          StructField("o", StringType)
        )
      )
    )
  }

  type M[A] = ReaderWriterStateT[Result, Config, Log, DataFrame @@ Untyped, A]
  val M = ReaderWriterStateT

  /** [[Phase]] represents a phase in the compiler.  It's parametrized
    * on the input type [[A]] and the output type [[B]].
    */
  type Phase[A, B] = Kleisli[M, A, B]
  val Phase = Kleisli

}
