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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

case class ArrayAppend(child: Expression, element: Expression) extends UnaryExpression {
  override def dataType: DataType = child.dataType
  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
  override def eval(input: InternalRow): Any = ???
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
}

case class ArrayInsert(child: Expression, pos: Expression, element: Expression) extends Expression {
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = Seq(child, pos, element)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = this
  override def eval(input: InternalRow): Any = ???
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???
}

object EvalMode {
  val LEGACY, ANSI, TRY = Value
  case class Value()
}

