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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}
import org.apache.spark.sql.execution.{CollectLimitExec, GlobalLimitExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.types.{DataType, StringType, TimestampNTZType}

import scala.collection.JavaConverters._

object CometShim extends CometShimBase {
  override def isTimestampNTZType(dt: DataType): Boolean = dt == TimestampNTZType
  override def getTimestampNTZType: DataType = TimestampNTZType
  override def isParquetFilterPushDownStringPredicate(conf: org.apache.spark.sql.internal.SQLConf): Boolean = true
  override def getExpressionProjectionCandidateLimit(conf: org.apache.spark.sql.internal.SQLConf): Int = 100
  override def projectExpression(
      expr: Expression,
      aliasMap: Map[Expression, Seq[Attribute]],
      outputSet: AttributeSet): Stream[Expression] = {
    Stream(expr.transformDown {
      case e: Expression if aliasMap.contains(e.canonicalized) =>
        aliasMap(e.canonicalized).head
    })
  }

  override def isAnsiMode(e: Expression): Boolean = e match {
    case a: Add => a.failOnError
    case s: Subtract => s.failOnError
    case m: Multiply => m.failOnError
    case d: Divide => d.failOnError
    case i: IntegralDivide => i.failOnError
    case r: Remainder => r.failOnError
    case _ => false
  }

  override def createSparkException(errorClass: String, messageParameters: Map[String, String], cause: Throwable): Exception = {
    new SparkException(s"[$errorClass] ${messageParameters.getOrElse("message", "")}", cause)
  }

  override def createSparkOutOfMemoryError(errorClass: String, messageParameters: java.util.Map[String, String]): Throwable = {
    new SparkOutOfMemoryError(errorClass, messageParameters.values().toArray(new Array[String](0)))
  }

  override def getOffset(plan: SparkPlan): Int = plan match {
    case _ => 0
  }

  override def broadcastInternal[T: scala.reflect.ClassTag](sc: SparkContext, value: T, serializedOnly: Boolean): org.apache.spark.broadcast.Broadcast[T] = {
    sc.broadcast(value)
  }

  override def getOrdering(plan: BatchScanExec): Option[Seq[SortOrder]] = None

  override def getKeyGroupedPartitioning(plan: BatchScanExec): Option[Seq[Expression]] = None
}

