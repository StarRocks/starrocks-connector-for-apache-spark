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
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

class StarRocksInternalRow(override val values: Array[Any])
  extends GenericInternalRow(values)
    with StarRocksGenericRow {

  /** No-arg constructor for Kryo serialization. */
  def this() = this(null)

  def getAs[T](ordinal: Int): T = genericGet(ordinal).asInstanceOf[T]

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

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    values.apply(ordinal) match {
      case row: InternalRow => row
      case arr: Array[_] => new StarRocksInternalRow(arr.asInstanceOf[Array[Any]])
      case _ => super.getStruct(ordinal, numFields)
    }
  }

  override def getArray(ordinal: Int): ArrayData = {
    values.apply(ordinal) match {
      case arrayData: ArrayData => arrayData
      case list: java.util.List[_] => new StarRocksArrayData(list.asScala.toArray[Any])
      case _ => super.getArray(ordinal)
    }
  }

  override def getMap(ordinal: Int): MapData = {
    values.apply(ordinal) match {
      case mapData: MapData => mapData
      case map: java.util.Map[_, _] =>
        val scalaMap = map.asScala.toMap
        val (keys, values) = scalaMap.unzip
        new ArrayBasedMapData(
          new StarRocksArrayData(keys.toArray[Any]),
          new StarRocksArrayData(values.toArray[Any])
        )
      case _ => super.getMap(ordinal)
    }
  }
}
