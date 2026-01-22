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

package org.apache.comet.vector;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;

/** A Comet column vector for fixed-size list type. */
public class CometFixedSizeListVector extends CometDecodedVector {
  final FixedSizeListVector fixedSizeListVector;
  final ValueVector dataVector;
  final ColumnVector dataColumnVector;
  final DictionaryProvider dictionaryProvider;
  final int listSize;

  public CometFixedSizeListVector(
      ValueVector vector, boolean useDecimal128, DictionaryProvider dictionaryProvider) {
    super(vector, vector.getField(), useDecimal128);

    this.fixedSizeListVector = ((FixedSizeListVector) vector);
    this.dataVector = fixedSizeListVector.getDataVector();
    this.dictionaryProvider = dictionaryProvider;
    this.listSize = fixedSizeListVector.getListSize();
    this.dataColumnVector = getVector(dataVector, useDecimal128, dictionaryProvider);
  }

  @Override
  public ColumnarArray getArray(int i) {
    // FixedSizeListVector has fixed-size lists, so we can calculate start/end directly
    int start = i * listSize;
    int end = (i + 1) * listSize;

    return new ColumnarArray(dataColumnVector, start, end - start);
  }

  @Override
  public CometVector slice(int offset, int length) {
    TransferPair tp = this.valueVector.getTransferPair(this.valueVector.getAllocator());
    tp.splitAndTransfer(offset, length);

    return new CometFixedSizeListVector(tp.getTo(), useDecimal128, dictionaryProvider);
  }
}
