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

package com.starrocks.connector.spark.rdd

import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_VALUE_READER_CLASS
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rest.PartitionDefinition

import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.internal.Logging
import org.apache.spark.{TaskContext, TaskKilledException}

private[spark] abstract class AbstractStarrocksRDDIterator[T](
    context: TaskContext,
    partition: PartitionDefinition) extends Iterator[T] with Logging {

  private var initialized = false
  private var closed = false

  // the reader obtain data from StarRocks BE
  lazy val reader = {
    initialized = true
    val settings = partition.settings()
    initReader(settings)
    val valueReaderName = settings.getProperty(STARROCKS_VALUE_READER_CLASS)
    logDebug(s"Use value reader '$valueReaderName'.")
    val cons = Class.forName(valueReaderName).getDeclaredConstructor(classOf[PartitionDefinition], classOf[Settings])
    cons.newInstance(partition, settings).asInstanceOf[ScalaValueReader]
  }

  context.addTaskCompletionListener(new TaskCompletionListener() {
    override def onTaskCompletion(context: TaskContext): Unit = {
      closeIfNeeded()
    }
  })

  override def hasNext: Boolean = {
    if (context.isInterrupted()) {
      throw new TaskKilledException
    }
    reader.hasNext
  }

  override def next(): T = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    reader.getNextRecord
    createValue(reader.getRow)
  }

  def closeIfNeeded(): Unit = {
    logTrace(s"Close status is '$closed' when close StarRocks RDD Iterator")
    if (!closed) {
      close()
      closed = true
    }
  }

  protected def close(): Unit = {
    logTrace(s"Initialize status is '$initialized' when close StarRocks RDD Iterator")
    if (initialized) {
      reader.close()
    }
  }

  def initReader(settings: Settings): Unit

  /**
   * convert value of row from reader.next return type to T.
   * @param value reader.next return value
   * @return value of type T
   */
  def createValue(value: Object): T
}
