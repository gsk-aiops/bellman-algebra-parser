package com.gsk.kg.engine

import cats.Foldable
import cats.data.NonEmptyList
import cats.implicits.toTraverseOps
import cats.instances.all._
import cats.syntax.applicative._
import cats.syntax.either._

import higherkindness.droste._
import higherkindness.droste.contrib.NewTypesSyntax.NewTypesOps
import higherkindness.droste.util.newtypes.@@

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row => SparkRow}

import com.gsk.kg.config.Config
import com.gsk.kg.engine.SPOEncoder._
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.engine.data.ChunkedList.Chunk
import com.gsk.kg.engine.functions.FuncAgg
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.engine.relational.Relational.Untyped
import com.gsk.kg.engine.relational.Relational.ops._
import com.gsk.kg.engine.relational.RelationalGrouped
import com.gsk.kg.sparqlparser.ConditionOrder.ASC
import com.gsk.kg.sparqlparser.ConditionOrder.DESC
import com.gsk.kg.sparqlparser.Expr.Quad
import com.gsk.kg.sparqlparser.Expr.Row
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal._
import com.gsk.kg.sparqlparser._

import java.{util => ju}

object Engine {

  def evaluateAlgebraM(implicit
      sc: SQLContext
  ): AlgebraM[M, DAG, Multiset[DataFrame @@ Untyped]] =
    AlgebraM[M, DAG, Multiset[DataFrame @@ Untyped]] {
      case DAG.Describe(vars, r) => evaluateDescribe(vars, r)
      case DAG.Ask(r)            => evaluateAsk(r)
      case DAG.Construct(bgp, r) => evaluateConstruct(bgp, r)
      case DAG.Scan(graph, expr) =>
        evaluateScan(graph, expr)
      case DAG.Project(variables, r) => r.select(variables: _*).pure[M]
      case DAG.Bind(variable, expression, r) =>
        evaluateBind(variable, expression, r)
      case DAG.Sequence(bps)           => evaluateSequence(bps)
      case DAG.Path(s, p, o, g, rev)   => evaluatePath(s, p, o, g, rev)
      case DAG.BGP(quads)              => evaluateBGP(quads)
      case DAG.LeftJoin(l, r, filters) => evaluateLeftJoin(l, r, filters)
      case DAG.Union(l, r)             => evaluateUnion(l, r)
      case DAG.Minus(l, r)             => evaluateMinus(l, r)
      case DAG.Filter(funcs, expr)     => evaluateFilter(funcs, expr)
      case DAG.Join(l, r)              => evaluateJoin(l, r)
      case DAG.Offset(offset, r)       => evaluateOffset(offset, r)
      case DAG.Limit(limit, r)         => evaluateLimit(limit, r)
      case DAG.Distinct(r)             => evaluateDistinct(r)
      case DAG.Reduced(r)              => evaluateReduced(r)
      case DAG.Group(vars, func, r)    => evaluateGroup(vars, func, r)
      case DAG.Order(conds, r)         => evaluateOrder(conds, r)
      case DAG.Table(vars, rows)       => evaluateTable(vars, rows)
      case DAG.Exists(not, p, r)       => evaluateExists(not, p, r)
      case DAG.Noop(str)               => evaluateNoop(str)
    }

  def evaluate[T: Basis[DAG, *]](
      dataframe: DataFrame,
      dag: T,
      config: Config
  )(implicit
      sc: SQLContext
  ): Result[DataFrame] = {
    val eval =
      scheme.cataM[M, DAG, T, Multiset[DataFrame @@ Untyped]](evaluateAlgebraM)

    validateInputDataFrame(@@(dataframe)).flatMap { df =>
      eval(dag)
        .runA(config, df)
        .map(_.relational.unwrap)
    }
  }

  private def validateInputDataFrame(
      df: DataFrame @@ Untyped
  ): Result[DataFrame @@ Untyped] = {
    val hasThreeOrFourColumns = df.columns.length == 3 || df.columns.length == 4

    for {
      _ <- Either.cond(
        hasThreeOrFourColumns,
        df,
        EngineError.InvalidInputDataFrame("Input DF must have 3 or 4 columns")
      )
      dataFrame =
        if (df.columns.length == 3) {
          df.withColumn("g", lit(""))
        } else {
          df
        }
    } yield dataFrame
  }

  private def evaluateNoop(
      str: String
  )(implicit sc: SQLContext): M[Multiset[DataFrame @@ Untyped]] =
    for {
      _ <- Log.info("Engine", str)
    } yield Multiset.empty

  private val createDescribeTriple: (StringVal, Int) => Quad = { (subject, i) =>
    def createVar(letter: String): VARIABLE = VARIABLE(s"?$letter$i")
    Quad(subject, createVar("p"), createVar("o"), Nil)
  }

  private def evaluateDescribe(
      vars: Seq[StringVal],
      r: Multiset[DataFrame @@ Untyped]
  )(implicit
      sc: SQLContext
  ): M[Multiset[DataFrame @@ Untyped]] = {
    val quads: ChunkedList[Quad] = ChunkedList
      .fromList(
        vars.toList.zipWithIndex.map(createDescribeTriple.tupled)
      )
    val bgp = Expr.BGP(Foldable[ChunkedList].toList(quads))

    M.get[Result, Config, Log, DataFrame @@ Untyped]
      .map { df =>
        quads
          .mapChunks { chunk =>
            val condition = composedConditionFromChunk(df, chunk)
            val filtered  = df.filter(condition)
            applyChunkToDf(chunk, filtered)
          }
          .foldLeft(Multiset.empty)(
            (
                acc: Multiset[DataFrame @@ Untyped],
                other: Multiset[DataFrame @@ Untyped]
            ) => acc.union(other)
          )
      }
      .flatMap(m => evaluateConstruct(bgp, m))
  }

  private def applyChunkToDf(
      chunk: ChunkedList.Chunk[Quad],
      filtered: DataFrame @@ Untyped
  )(implicit
      sc: SQLContext
  ): Multiset[DataFrame @@ Untyped] = {
    import sc.implicits._
    val vars =
      chunk
        .map(_.getNamesAndPositions :+ (GRAPH_VARIABLE, "g"))
        .toChain
        .toList
        .flatten
    val selected =
      filtered.select(vars.map(v => $"${v._2}".as(v._1.s)))

    Multiset[DataFrame @@ Untyped](
      vars.map {
        case (GRAPH_VARIABLE, _) =>
          VARIABLE(GRAPH_VARIABLE.s)
        case (other, _) =>
          other.asInstanceOf[VARIABLE]
      }.toSet,
      selected
    )
  }

  private def evaluateAsk(
      r: Multiset[DataFrame @@ Untyped]
  )(implicit sc: SQLContext): M[Multiset[DataFrame @@ Untyped]] = {
    val askVariable = VARIABLE("?_askResult")
    val isEmpty     = !r.relational.isEmpty
    val schema      = StructType(Seq(StructField(askVariable.s, BooleanType, false)))
    val rows        = Seq(SparkRow(isEmpty))
    val askDf = @@[DataFrame, Untyped](
      sc.sparkSession.createDataFrame(sc.sparkContext.parallelize(rows), schema)
    )

    Multiset[DataFrame @@ Untyped](
      bindings = Set(askVariable),
      relational = askDf
    )
  }.pure[M]

  private def evaluateJoin(
      l: Multiset[DataFrame @@ Untyped],
      r: Multiset[DataFrame @@ Untyped]
  )(implicit
      sc: SQLContext
  ): M[Multiset[DataFrame @@ Untyped]] =
    l.join(r).pure[M]

  private def evaluateUnion(
      l: Multiset[DataFrame @@ Untyped],
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] =
    l.union(r).pure[M]

  private def evaluateMinus(
      l: Multiset[DataFrame @@ Untyped],
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] =
    l.minus(r).pure[M]

  private def evaluateScan(
      graph: String,
      expr: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] = {
    val bindings =
      expr.bindings.filter(_.s != GRAPH_VARIABLE.s) + VARIABLE(graph)

    val graphColumnMetadata =
      new MetadataBuilder().putString("graph_column_name", graph).build()
    val updatedGraphCol =
      col(GRAPH_VARIABLE.s).as("graph_column", graphColumnMetadata)

    val df = expr.relational
      .withColumn(graph, updatedGraphCol)

    Multiset[DataFrame @@ Untyped](
      bindings,
      df
    )
  }.pure[M]

  private def evaluateSequence(bps: List[Multiset[DataFrame @@ Untyped]])(
      implicit sc: SQLContext
  ): M[Multiset[DataFrame @@ Untyped]] =
    Foldable[List].fold(bps).pure[M]

  private def evaluatePath(
      s: StringVal,
      p: PropertyExpression,
      o: StringVal,
      g: List[StringVal],
      rev: Boolean
  )(implicit sc: SQLContext): M[Multiset[DataFrame @@ Untyped]] = {
    M.get[Result, Config, Log, DataFrame @@ Untyped].flatMap { df =>
      M.ask[Result, Config, Log, DataFrame @@ Untyped].flatMapF { config =>
        PropertyExpressionF
          .compile[PropertyExpression](p, config)
          .apply(df)
          .map {
            case Right(accDf) =>
              val renamedDf = accDf
              val chunk = if (rev) {
                Chunk(Quad(o, STRING(""), s, List.empty))
              } else {
                Chunk(Quad(s, STRING(""), o, List.empty))
              }
              val result = applyChunkToDf(chunk, renamedDf)
              result.copy(relational =
                result.relational
                  .withColumnRenamed("s", s.s)
                  .withColumnRenamed("o", o.s)
              )
            case Left(cond) =>
              val chunk = if (rev) {
                Chunk(Quad(o, STRING(""), s, List.empty))
              } else {
                Chunk(Quad(s, STRING(""), o, List.empty))
              }
              val filtered = df.filter(cond)
              applyChunkToDf(chunk, filtered)
          }
      }
    }
  }

  private def evaluateBGP(
      quads: ChunkedList[Expr.Quad]
  )(implicit sc: SQLContext): M[Multiset[DataFrame @@ Untyped]] =
    M.get[Result, Config, Log, DataFrame @@ Untyped].map { df =>
      Foldable[ChunkedList].fold(
        quads.mapChunks { chunk =>
          val condition = composedConditionFromChunk(df, chunk)
          val filtered  = df.filter(condition)
          applyChunkToDf(chunk, filtered)
        }
      )
    }

  /** This method takes all the predicates from a chunk of Quads and generates a Spark condition as
    * a Column with the next constraints:
    * - Predicates on same column are composed with OR operations between conditions. Eg:
    *   (col1, List(p1, p2)) => (false OR (p1 OR p2))
    * - Predicates on different columns are composed with AND operations between conditions. Eg:
    *   ((col1, List(p1)), (col2, List(p2)) => (true && (p1 && p2))
    * - Predicates in the same chunk are composed with OR operation. Eg:
    *   (c1 -> (true && p1 && (p2 || p3)), c2 -> (true && p4)) =>
    *     (false || ((true && p1 && (p2 || p3)) || (true && p4)))
    * @param relational
    * @param chunk
    * @return
    */
  def composedConditionFromChunk(
      relational: DataFrame @@ Untyped,
      chunk: Chunk[Quad]
  ): Column = {
    chunk
      .map { quad =>
        quad.getPredicates
          .groupBy(_._2)
          .map { case (_, vs) =>
            vs.map { case (pred, position) =>
              val col = relational.getColumn(position)
              when(
                col.startsWith("\"") && col.endsWith("\""),
                FuncForms.equals(trim(col, "\""), lit(pred.s))
              ).otherwise(FuncForms.equals(col, lit(pred.s)))
            }.foldLeft(lit(false))(_ || _)
          }
          .foldLeft(lit(true))(_ && _)
      }
      .foldLeft(lit(true))(_ && _)
  }

  private def evaluateDistinct(
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] =
    M.liftF(r.distinct)

  private def evaluateReduced(
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] =
    // It is up to the implementation to eliminate duplicates or not.
    // See: https://www.w3.org/TR/sparql11-query/#modReduced
    M.liftF(r.distinct)

  private def evaluateGroup(
      vars: List[VARIABLE],
      func: List[(VARIABLE, Expression)],
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] = {
    val groupedDF: RelationalGroupedDataset @@ Untyped =
      RelationalGrouped
        .Aux[DataFrame @@ Untyped, RelationalGroupedDataset @@ Untyped]
        .groupBy(
          r.relational,
          (vars :+ VARIABLE(GRAPH_VARIABLE.s)).map(_.s).map(col)
        )

    evaluateAggregation(
      vars :+ VARIABLE(GRAPH_VARIABLE.s),
      groupedDF.unwrap,
      func
    )
      .map(df =>
        r.copy(
          relational = df.@@,
          bindings =
            r.bindings.union(func.toSet[(VARIABLE, Expression)].map(x => x._1))
        )
      )
  }

  private def toColumnOperation(
      func: (VARIABLE, Expression)
  ): M[Column] = func match {
    case (VARIABLE(name), Aggregate.COUNT(VARIABLE(v))) =>
      FuncAgg.countAgg(col(v)).cast("string").as(name).pure[M]
    case (VARIABLE(name), Aggregate.SUM(VARIABLE(v))) =>
      FuncAgg.sumAgg(col(v)).cast("string").as(name).pure[M]
    case (VARIABLE(name), Aggregate.MIN(VARIABLE(v))) =>
      FuncAgg.minAgg(col(v)).cast("string").as(name).pure[M]
    case (VARIABLE(name), Aggregate.MAX(VARIABLE(v))) =>
      FuncAgg.maxAgg(col(v)).cast("string").as(name).pure[M]
    case (VARIABLE(name), Aggregate.AVG(VARIABLE(v))) =>
      FuncAgg.avgAgg(col(v)).cast("string").as(name).pure[M]
    case (VARIABLE(name), Aggregate.SAMPLE(VARIABLE(v))) =>
      FuncAgg.sample(col(v)).as(name).pure[M]
    case (VARIABLE(name), Aggregate.GROUP_CONCAT(VARIABLE(v), separator)) =>
      FuncAgg.groupConcat(col(v).as(name), separator).pure[M]
    case fn =>
      M.liftF[Result, Config, Log, DataFrame @@ Untyped, Column](
        EngineError
          .UnknownFunction("Aggregate function: " + fn.toString)
          .asLeft
      )
  }

  private def evaluateAggregation(
      vars: List[VARIABLE],
      df: RelationalGroupedDataset,
      func: List[(VARIABLE, Expression)]
  ): M[DataFrame] = func match {
    case Nil =>
      val cols: List[Column] = vars.map(_.s).map(col).map(FuncAgg.sample)
      df.agg(cols.head, cols.tail: _*).pure[M]
    case agg :: Nil =>
      toColumnOperation(agg).map(df.agg(_))
    case aggs =>
      aggs
        .traverse(toColumnOperation)
        .map(columns => df.agg(columns.head, columns.tail: _*))
  }

  private def evaluateOrder(
      conds: NonEmptyList[ConditionOrder],
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] = {
    M.ask[Result, Config, Log, DataFrame @@ Untyped].flatMapF { config =>
      conds
        .map {
          case ASC(VARIABLE(v)) =>
            col(v).asc.asRight
          case ASC(e) =>
            ExpressionF
              .compile[Expression](e, config)
              .apply(r.relational)
              .map(_.asc)
          case DESC(VARIABLE(v)) =>
            col(v).desc.asRight
          case DESC(e) =>
            ExpressionF
              .compile[Expression](e, config)
              .apply(r.relational)
              .map(_.desc)
        }
        .toList
        .sequence[Either[EngineError, *], Column]
        .map(columns => r.copy(relational = r.relational.orderBy(columns)))
    }
  }

  private def evaluateLeftJoin(
      l: Multiset[DataFrame @@ Untyped],
      r: Multiset[DataFrame @@ Untyped],
      filters: List[Expression]
  ): M[Multiset[DataFrame @@ Untyped]] = {
    NonEmptyList
      .fromList(filters)
      .map { nelFilters =>
        evaluateFilter(nelFilters, r).flatMapF(l.leftJoin)
      }
      .getOrElse {
        M.liftF(l.leftJoin(r))
      }
  }

  private def evaluateFilter(
      funcs: NonEmptyList[Expression],
      expr: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] = {
    val compiledFuncs: M[NonEmptyList[DataFrame @@ Untyped => Result[Column]]] =
      M.ask[Result, Config, Log, DataFrame @@ Untyped].map { config =>
        funcs.map(t => ExpressionF.compile[Expression](t, config))
      }

    compiledFuncs.flatMapF(
      _.foldLeft(expr.asRight: Result[Multiset[DataFrame @@ Untyped]]) {
        case (eitherAcc, f) =>
          for {
            acc       <- eitherAcc
            filterCol <- f(acc.relational)
            result <-
              expr
                .filter(filterCol)
                .map(r =>
                  expr.copy(relational = r.relational intersect acc.relational)
                )
          } yield result
      }
    )
  }

  private def evaluateOffset(
      offset: Long,
      r: Multiset[DataFrame @@ Untyped]
  )(implicit sc: SQLContext): M[Multiset[DataFrame @@ Untyped]] =
    M.liftF(r.offset(offset))

  private def evaluateLimit(
      limit: Long,
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] =
    M.liftF(r.limit(limit))

  /** Evaluate a construct expression.
    *
    * Something we do in this that differs from the spec is that we
    * apply a default ordering to all solutions generated by the
    * [[bgp]], so that LIMIT and OFFSET can return meaningful results.
    */
  private def evaluateConstructPlain(
      bgp: Expr.BGP,
      r: Multiset[DataFrame @@ Untyped]
  )(implicit
      sc: SQLContext
  ): Multiset[DataFrame @@ Untyped] = {

    // Extracting the triples to something that can be serialized in
    // Spark jobs
    val templateValues: List[List[(StringVal, Int)]] =
      bgp.quads
        .map(quad => List(quad.s -> 1, quad.p -> 2, quad.o -> 3))
        .toList

    val df = r.relational.flatMap { solution =>
      val extractBlanks: List[(StringVal, Int)] => List[StringVal] =
        triple => triple.filter(x => x._1.isBlank).map(_._1)

      val blankNodes: Map[String, String] =
        templateValues
          .flatMap(extractBlanks)
          .distinct
          .map(blankLabel => (blankLabel.s, ju.UUID.randomUUID().toString()))
          .toMap

      templateValues.map { triple =>
        val fields: List[Any] = triple
          .map({
            case (VARIABLE(s), pos) =>
              (solution.get(solution.fieldIndex(s)), pos)
            case (BLANK(x), pos) =>
              (blankNodes.get(x).get, pos)
            case (x, pos) =>
              (x.s, pos)
          })
          .sortBy(_._2)
          .map(_._1)

        SparkRow.fromSeq(fields)
      }
    }.distinct

    Multiset[DataFrame @@ Untyped](
      Set.empty,
      df
    )
  }

  private def evaluateConstruct(
      bgp: Expr.BGP,
      r: Multiset[DataFrame @@ Untyped]
  )(implicit
      sc: SQLContext
  ): M[Multiset[DataFrame @@ Untyped]] =
    evaluateConstructPlain(bgp, r).pure[M]

  private def evaluateBind(
      bindTo: VARIABLE,
      bindFrom: Expression,
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] = {
    M.ask[Result, Config, Log, DataFrame @@ Untyped].flatMapF { config =>
      val getColumn = ExpressionF.compile(bindFrom, config)
      getColumn(r.relational).map { col =>
        r.withColumn(bindTo, col)
      }
    }
  }

  private def evaluateTable(
      vars: List[VARIABLE],
      rows: List[Row]
  )(implicit sc: SQLContext): M[Multiset[DataFrame @@ Untyped]] = {

    def parseRow(totalVars: Seq[VARIABLE], row: Row): SparkRow = {
      SparkRow.fromSeq(totalVars.foldLeft(Seq.empty[String]) { case (acc, v) =>
        val parsed = row.tuples
          .groupBy(_._1.s)
          .mapValues(_.map(_._2.s))
          .getOrElse(v.s, Seq(null)) // scalastyle:ignore
        acc ++ parsed
      } :+ "")
    }

    val sparkRows = rows.map(r => parseRow(vars, r))
    val schema = StructType(
      vars
        .map(name => StructField(name.s, StringType, true)) :+
        StructField(GRAPH_VARIABLE.s, StringType, false)
    )

    val df = @@[DataFrame, Untyped](
      sc.sparkSession.createDataFrame(
        sc.sparkContext.parallelize(sparkRows),
        schema
      )
    )

    Multiset[DataFrame @@ Untyped](
      bindings = (vars :+ VARIABLE(GRAPH_VARIABLE.s)).toSet,
      relational = df
    )
  }.pure[M]

  private def evaluateExists(
      not: Boolean,
      p: Multiset[DataFrame @@ Untyped],
      r: Multiset[DataFrame @@ Untyped]
  ): M[Multiset[DataFrame @@ Untyped]] = {
    val cols = p.relational.columns intersect r.relational.columns

    val resultDf = if (!not) {
      // left semi join will return a copy of each row in the left dataframe for which a match is found in
      // the right dataframe. This means that it will detect the presence of matches between the two dataframes.
      r.relational.leftSemi(p.relational, cols)
    } else {
      // left anti join can be defined as the complementary operation of the left semi join. It will return one copy
      // of the left dataframe for which no match is found on the right dataframe. This means that it will
      // detect the absence of a match between the two dataframes.
      r.relational.leftAnti(p.relational, cols)
    }

    r.copy(
      relational = resultDf
    ).pure[M]
  }
}
