package com.starrocks.connector.spark.sql

import com.alibaba.fastjson2.{JSON, JSONObject}
import com.starrocks.data.load.stream.StreamLoadManager
import com.starrocks.data.load.stream.DefaultStreamLoadManager
import com.starrocks.data.load.stream.properties.StreamLoadProperties
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.slf4j.{Logger, LoggerFactory}


private[sql] class StarRocksSink private() extends Sink with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[StarRocksSink].getName)

  @volatile private var latestBatchId = -1L
  var sqlContext: SQLContext = _
  var props: StreamLoadProperties = _
  var sinkManager: StreamLoadManager = _

  def this(sqlContext: SQLContext, props: StreamLoadProperties) {
    this()
    this.sqlContext = sqlContext
    this.props = props
    this.sinkManager = new DefaultStreamLoadManager(props)

//    sinkManager.init()
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

  }
}
