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

package org.apache.comet.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StructField, StructType}

case class ReduceEtd(
    child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends ImperativeAggregate {
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = true
  override def dataType: DataType = ArrayType(LongType)
  override val aggBufferAttributes: Seq[AttributeReference] =
    Seq(
      AttributeReference("et_early", LongType, nullable = true)(),
      AttributeReference("et_latest", LongType, nullable = true)())
  override val aggBufferSchema: StructType =
    StructType(
      Seq(
        StructField("et_early", LongType, nullable = true),
        StructField("et_latest", LongType, nullable = true)))
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes
  override def prettyName: String = "reduce_etd"

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def initialize(buffer: InternalRow): Unit = {
    buffer.setNullAt(mutableAggBufferOffset)
    buffer.setNullAt(mutableAggBufferOffset + 1)
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val value = child.eval(input)
    if (value != null) {
      val ts = value.asInstanceOf[Long]
      if (buffer.isNullAt(mutableAggBufferOffset)) {
        buffer.setLong(mutableAggBufferOffset, ts)
        buffer.setLong(mutableAggBufferOffset + 1, ts)
      } else {
        val early = buffer.getLong(mutableAggBufferOffset)
        val latest = buffer.getLong(mutableAggBufferOffset + 1)
        if (ts < early) buffer.setLong(mutableAggBufferOffset, ts)
        if (ts > latest) buffer.setLong(mutableAggBufferOffset + 1, ts)
      }
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    if (!inputBuffer.isNullAt(inputAggBufferOffset)) {
      val inputEarly = inputBuffer.getLong(inputAggBufferOffset)
      val inputLatest = inputBuffer.getLong(inputAggBufferOffset + 1)
      if (buffer.isNullAt(mutableAggBufferOffset)) {
        buffer.setLong(mutableAggBufferOffset, inputEarly)
        buffer.setLong(mutableAggBufferOffset + 1, inputLatest)
      } else {
        val early = buffer.getLong(mutableAggBufferOffset)
        val latest = buffer.getLong(mutableAggBufferOffset + 1)
        if (inputEarly < early) buffer.setLong(mutableAggBufferOffset, inputEarly)
        if (inputLatest > latest) buffer.setLong(mutableAggBufferOffset + 1, inputLatest)
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(mutableAggBufferOffset)) {
      null
    } else {
      ArrayData.toArrayData(
        Array(input.getLong(mutableAggBufferOffset), input.getLong(mutableAggBufferOffset + 1)))
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val thisObj = ctx.addReferenceObj("this", this, this.getClass.getName)
    val javaType = CodeGenerator.javaType(dataType)
    val defaultVal = CodeGenerator.defaultValue(dataType)
    ev.copy(code = code"""
      boolean ${ev.isNull} = $thisObj.isNullAt($mutableAggBufferOffset);
      $javaType ${ev.value} = $defaultVal;
      if (!${ev.isNull}) {
        ${ev.value} = ($javaType) $thisObj.eval(null);
      }
    """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ReduceEtd =
    copy(child = newChildren.head)
}

case class PartialEtd(
    child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends ImperativeAggregate {
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = true
  override def dataType: DataType = ArrayType(LongType)
  override val aggBufferAttributes: Seq[AttributeReference] =
    Seq(
      AttributeReference("et_early", LongType, nullable = true)(),
      AttributeReference("et_latest", LongType, nullable = true)())
  override val aggBufferSchema: StructType =
    StructType(
      Seq(
        StructField("et_early", LongType, nullable = true),
        StructField("et_latest", LongType, nullable = true)))
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes
  override def prettyName: String = "partial_etd"

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def initialize(buffer: InternalRow): Unit = {
    buffer.setNullAt(mutableAggBufferOffset)
    buffer.setNullAt(mutableAggBufferOffset + 1)
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val value = child.eval(input)
    if (value != null) {
      val inputState = value.asInstanceOf[ArrayData]
      if (!inputState.isNullAt(0)) {
        val inputEarly = inputState.getLong(0)
        val inputLatest = inputState.getLong(1)
        if (buffer.isNullAt(mutableAggBufferOffset)) {
          buffer.setLong(mutableAggBufferOffset, inputEarly)
          buffer.setLong(mutableAggBufferOffset + 1, inputLatest)
        } else {
          val early = buffer.getLong(mutableAggBufferOffset)
          val latest = buffer.getLong(mutableAggBufferOffset + 1)
          if (inputEarly < early) buffer.setLong(mutableAggBufferOffset, inputEarly)
          if (inputLatest > latest) buffer.setLong(mutableAggBufferOffset + 1, inputLatest)
        }
      }
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    if (!inputBuffer.isNullAt(inputAggBufferOffset)) {
      val inputEarly = inputBuffer.getLong(inputAggBufferOffset)
      val inputLatest = inputBuffer.getLong(inputAggBufferOffset + 1)
      if (buffer.isNullAt(mutableAggBufferOffset)) {
        buffer.setLong(mutableAggBufferOffset, inputEarly)
        buffer.setLong(mutableAggBufferOffset + 1, inputLatest)
      } else {
        val early = buffer.getLong(mutableAggBufferOffset)
        val latest = buffer.getLong(mutableAggBufferOffset + 1)
        if (inputEarly < early) buffer.setLong(mutableAggBufferOffset, inputEarly)
        if (inputLatest > latest) buffer.setLong(mutableAggBufferOffset + 1, inputLatest)
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(mutableAggBufferOffset)) {
      null
    } else {
      ArrayData.toArrayData(
        Array(input.getLong(mutableAggBufferOffset), input.getLong(mutableAggBufferOffset + 1)))
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val thisObj = ctx.addReferenceObj("this", this, this.getClass.getName)
    val javaType = CodeGenerator.javaType(dataType)
    val defaultVal = CodeGenerator.defaultValue(dataType)
    ev.copy(code = code"""
      boolean ${ev.isNull} = $thisObj.isNullAt($mutableAggBufferOffset);
      $javaType ${ev.value} = $defaultVal;
      if (!${ev.isNull}) {
        ${ev.value} = ($javaType) $thisObj.eval(null);
      }
    """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): PartialEtd =
    copy(child = newChildren.head)
}

case class FinalEtd(col: Expression, ts: Expression, isRecent: Expression) extends Expression {
  override def children: Seq[Expression] = Seq(col, ts, isRecent)
  override def nullable: Boolean = true
  override def dataType: DataType = LongType
  override def prettyName: String = "final_etd"

  override def eval(input: InternalRow): Any = {
    val colVal = col.eval(input).asInstanceOf[ArrayData]
    val tsVal = ts.eval(input)
    val isRecentVal = isRecent.eval(input)
    if (colVal == null || tsVal == null || isRecentVal == null || colVal.numElements() < 2) {
      null
    } else {
      val early = colVal.getLong(0)
      val latest = colVal.getLong(1)
      val currentTs = tsVal.asInstanceOf[Long]
      if (isRecentVal.asInstanceOf[Boolean]) currentTs - latest else currentTs - early
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val thisObj = ctx.addReferenceObj("this", this, this.getClass.getName)
    val colEval = col.genCode(ctx)
    val tsEval = ts.genCode(ctx)
    val isRecentEval = isRecent.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val defaultVal = CodeGenerator.defaultValue(dataType)

    ev.copy(code = code"""
      ${colEval.code}
      ${tsEval.code}
      ${isRecentEval.code}
      boolean ${ev.isNull} = ${colEval.isNull} || ${tsEval.isNull} || ${isRecentEval.isNull};
      $javaType ${ev.value} = $defaultVal;
      if (!${ev.isNull}) {
        ArrayData colVal = (ArrayData) ${colEval.value};
        if (colVal.numElements() < 2) {
          ${ev.isNull} = true;
        } else {
          long early = colVal.getLong(0);
          long latest = colVal.getLong(1);
          long currentTs = (long) ${tsEval.value};
          if ((boolean) ${isRecentEval.value}) {
            ${ev.value} = currentTs - latest;
          } else {
            ${ev.value} = currentTs - early;
          }
        }
      }
    """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): FinalEtd =
    copy(col = newChildren(0), ts = newChildren(1), isRecent = newChildren(2))
}
