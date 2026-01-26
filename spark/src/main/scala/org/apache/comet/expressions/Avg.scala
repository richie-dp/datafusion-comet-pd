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
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StructField, StructType}

case class ReduceAvg(
    child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends ImperativeAggregate {
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = true
  override def dataType: DataType = ArrayType(DoubleType)
  override val aggBufferAttributes: Seq[AttributeReference] =
    Seq(
      AttributeReference("count", DoubleType, nullable = true)(),
      AttributeReference("sum", DoubleType, nullable = true)())
  override val aggBufferSchema: StructType =
    StructType(
      Seq(
        StructField("count", DoubleType, nullable = true),
        StructField("sum", DoubleType, nullable = true)))
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes
  override def prettyName: String = "reduce_avg"

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
      // Convert various numeric types to Double
      val v: Double = value match {
        case d: Double => d
        case f: Float => f.toDouble
        case l: Long => l.toDouble
        case i: Int => i.toDouble
        case s: Short => s.toDouble
        case b: Byte => b.toDouble
        case _ => value.toString.toDouble
      }
      if (buffer.isNullAt(mutableAggBufferOffset)) {
        buffer.setDouble(mutableAggBufferOffset, 1.0) // count = 1
        buffer.setDouble(mutableAggBufferOffset + 1, v) // sum = v
      } else {
        val count = buffer.getDouble(mutableAggBufferOffset)
        val sum = buffer.getDouble(mutableAggBufferOffset + 1)
        buffer.setDouble(mutableAggBufferOffset, count + 1.0)
        buffer.setDouble(mutableAggBufferOffset + 1, sum + v)
      }
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    if (!inputBuffer.isNullAt(inputAggBufferOffset)) {
      val inputCount = inputBuffer.getDouble(inputAggBufferOffset)
      val inputSum = inputBuffer.getDouble(inputAggBufferOffset + 1)
      if (buffer.isNullAt(mutableAggBufferOffset)) {
        buffer.setDouble(mutableAggBufferOffset, inputCount)
        buffer.setDouble(mutableAggBufferOffset + 1, inputSum)
      } else {
        val count = buffer.getDouble(mutableAggBufferOffset)
        val sum = buffer.getDouble(mutableAggBufferOffset + 1)
        buffer.setDouble(mutableAggBufferOffset, count + inputCount)
        buffer.setDouble(mutableAggBufferOffset + 1, sum + inputSum)
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(mutableAggBufferOffset)) {
      null
    } else {
      ArrayData.toArrayData(
        Array(
          input.getDouble(mutableAggBufferOffset),
          input.getDouble(mutableAggBufferOffset + 1)))
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

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ReduceAvg =
    copy(child = newChildren.head)
}

case class FinalAvg(
    col: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends ImperativeAggregate {
  override def children: Seq[Expression] = Seq(col)
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override val aggBufferAttributes: Seq[AttributeReference] =
    Seq(
      AttributeReference("count", DoubleType, nullable = true)(),
      AttributeReference("sum", DoubleType, nullable = true)())
  override val aggBufferSchema: StructType =
    StructType(
      Seq(
        StructField("count", DoubleType, nullable = true),
        StructField("sum", DoubleType, nullable = true)))
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes
  override def prettyName: String = "final_avg"

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override def initialize(buffer: InternalRow): Unit = {
    buffer.setNullAt(mutableAggBufferOffset)
    buffer.setNullAt(mutableAggBufferOffset + 1)
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val value = col.eval(input)
    if (value != null) {
      val arrayData = value.asInstanceOf[ArrayData]
      if (arrayData.numElements() >= 2 && !arrayData.isNullAt(0) && !arrayData.isNullAt(1)) {
        val inputCount = arrayData.getDouble(0)
        val inputSum = arrayData.getDouble(1)
        if (buffer.isNullAt(mutableAggBufferOffset)) {
          buffer.setDouble(mutableAggBufferOffset, inputCount)
          buffer.setDouble(mutableAggBufferOffset + 1, inputSum)
        } else {
          val count = buffer.getDouble(mutableAggBufferOffset)
          val sum = buffer.getDouble(mutableAggBufferOffset + 1)
          buffer.setDouble(mutableAggBufferOffset, count + inputCount)
          buffer.setDouble(mutableAggBufferOffset + 1, sum + inputSum)
        }
      }
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    if (!inputBuffer.isNullAt(inputAggBufferOffset)) {
      val inputCount = inputBuffer.getDouble(inputAggBufferOffset)
      val inputSum = inputBuffer.getDouble(inputAggBufferOffset + 1)
      if (buffer.isNullAt(mutableAggBufferOffset)) {
        buffer.setDouble(mutableAggBufferOffset, inputCount)
        buffer.setDouble(mutableAggBufferOffset + 1, inputSum)
      } else {
        val count = buffer.getDouble(mutableAggBufferOffset)
        val sum = buffer.getDouble(mutableAggBufferOffset + 1)
        buffer.setDouble(mutableAggBufferOffset, count + inputCount)
        buffer.setDouble(mutableAggBufferOffset + 1, sum + inputSum)
      }
    }
  }

  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(mutableAggBufferOffset)) {
      null
    } else {
      val count = input.getDouble(mutableAggBufferOffset)
      val sum = input.getDouble(mutableAggBufferOffset + 1)
      if (count == 0) {
        null
      } else {
        sum / count
      }
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
        double count = $thisObj.getDouble($mutableAggBufferOffset);
        double sum = $thisObj.getDouble($mutableAggBufferOffset + 1);
        if (count == 0) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = sum / count;
        }
      }
    """)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): FinalAvg =
    copy(col = newChildren(0))
}
