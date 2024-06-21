// Modifications Copyright 2021 StarRocks Limited.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.read

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String

class StarRocksInternalRow(override val values: Array[Any])
  extends GenericInternalRow(values)
    with StarRocksGenericRow {

  /** No-arg constructor for Kryo serialization. */
  def this() = this(null)

  def getAs[T](ordinal: Int) = genericGet(ordinal).asInstanceOf[T]

  override def getLong(ordinal: Int): Long = {
    values.apply(ordinal) match {
      case d: java.sql.Timestamp => DateTimeUtils.fromJavaTimestamp(d)
      case _ => super.getLong(ordinal)
    }
  }

  override def getInt(ordinal: Int): Int = {
    values.apply(ordinal) match {
      case d: java.sql.Date => DateTimeUtils.anyToDays(d)
      case _ => super.getInt(ordinal)
    }
  }

  override def getUTF8String(ordinal: Int): UTF8String = UTF8String.fromString(getAs[String](ordinal))
}
