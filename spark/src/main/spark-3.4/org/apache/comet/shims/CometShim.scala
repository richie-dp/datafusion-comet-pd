/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.shims

import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{DataType, TimestampNTZType}

object CometShim extends CometShimBase {
  override def isTimestampNTZType(dt: DataType): Boolean = dt == TimestampNTZType
  override def getTimestampNTZType: DataType = TimestampNTZType
  override def isParquetFilterPushDownStringPredicate(conf: org.apache.spark.sql.internal.SQLConf): Boolean =
    conf.parquetFilterPushDownStringPredicate
  override def getExpressionProjectionCandidateLimit(conf: org.apache.spark.sql.internal.SQLConf): Int =
    conf.getConf(org.apache.spark.sql.internal.SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT)

  override def projectExpression(
      expr: Expression,
      aliasMap: Map[Expression, Seq[Attribute]],
      outputSet: AttributeSet): Stream[Expression] = {
    expr.multiTransformDown {
      // Mapping with aliases
      case e: Expression if aliasMap.contains(e.canonicalized) =>
        aliasMap(e.canonicalized) ++ (if (e.children.nonEmpty) Seq(e) else Seq.empty)

      // Prune if we encounter an attribute that we can't map and it is not in output set.
      // This prune will go up to the closest `multiTransformDown()` call and returns `Stream.empty`
      // there.
      case a: Attribute if !outputSet.contains(a) => Seq.empty
    }.toStream
  }

  override def isAnsiMode(e: Expression): Boolean = e match {
    case a: Add => a.evalMode == EvalMode.ANSI
    case s: Subtract => s.evalMode == EvalMode.ANSI
    case m: Multiply => m.evalMode == EvalMode.ANSI
    case d: Divide => d.evalMode == EvalMode.ANSI
    case i: IntegralDivide => i.evalMode == EvalMode.ANSI
    case r: Remainder => r.evalMode == EvalMode.ANSI
    case a: org.apache.spark.sql.catalyst.expressions.aggregate.Average => a.evalMode == EvalMode.ANSI
    case s: org.apache.spark.sql.catalyst.expressions.aggregate.Sum => s.evalMode == EvalMode.ANSI
    case _ => false
  }

  override def createSparkException(errorClass: String, messageParameters: Map[String, String], cause: Throwable): Exception = {
    new SparkException(errorClass, messageParameters, cause)
  }

  override def createSparkOutOfMemoryError(errorClass: String, messageParameters: java.util.Map[String, String]): Throwable = {
    new SparkOutOfMemoryError(errorClass, messageParameters)
  }

  override def getOffset(plan: SparkPlan): Int = plan match {
    case g: GlobalLimitExec => g.offset
    case c: CollectLimitExec => c.offset
    case t: TakeOrderedAndProjectExec => t.offset
    case _ => 0
  }

  override def broadcastInternal[T: scala.reflect.ClassTag](sc: SparkContext, value: T, serializedOnly: Boolean): org.apache.spark.broadcast.Broadcast[T] = {
    sc.broadcastInternal(value, serializedOnly)
  }

  override def getOrdering(plan: BatchScanExec): Option[Seq[SortOrder]] = plan.ordering

  override def getKeyGroupedPartitioning(plan: BatchScanExec): Option[Seq[Expression]] = plan.keyGroupedPartitioning
}

