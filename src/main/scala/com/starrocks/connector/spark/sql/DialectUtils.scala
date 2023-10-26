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

package com.starrocks.connector.spark.sql

import com.starrocks.connector.spark.cfg.ConfigurationOptions
import com.starrocks.connector.spark.exception.StarRocksException
import com.starrocks.connector.spark.sql.conf.StarRocksConfigBase.KEY_FE_HTTP
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources._
import org.slf4j.Logger

private[spark] object DialectUtils {
  /**
   * quote column name
   * @param colName column name
   * @return quoted column name
   */
  def quote(colName: String): String = s"`$colName`"

  /**
   * compile a filter to StarRocks FE filter format.
   * @param filter filter to be compile
   * @param dialect jdbc dialect to translate value to sql format
   * @param inValueLengthLimit max length of in value array
   * @return if StarRocks FE can handle this filter, return None if StarRocks FE can not handled it.
   */
  def compileFilter(filter: Filter, dialect: JdbcDialect, inValueLengthLimit: Int): Option[String] = {
    Option(filter match {
      case EqualTo(attribute, value) => s"${quote(attribute)} = ${dialect.compileValue(value)}"
      case GreaterThan(attribute, value) => s"${quote(attribute)} > ${dialect.compileValue(value)}"
      case GreaterThanOrEqual(attribute, value) => s"${quote(attribute)} >= ${dialect.compileValue(value)}"
      case LessThan(attribute, value) => s"${quote(attribute)} < ${dialect.compileValue(value)}"
      case LessThanOrEqual(attribute, value) => s"${quote(attribute)} <= ${dialect.compileValue(value)}"
      case In(attribute, values) =>
        if (values.isEmpty || values.length >= inValueLengthLimit) {
          null
        } else {
          s"${quote(attribute)} in (${dialect.compileValue(values)})"
        }
      case IsNull(attribute) => s"${quote(attribute)} is null"
      case IsNotNull(attribute) => s"${quote(attribute)} is not null"
      case And(left, right) =>
        val and = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" and ")
        } else {
          null
        }
      case Or(left, right) =>
        val or = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" or ")
        } else {
          null
        }
      case _ => null
    })
  }

  /**
   * check parameters validation and process it.
   * @param parameters parameters from rdd and spark conf
   * @param logger slf4j logger
   * @return processed parameters
   */
  def params(parameters: Map[String, String], logger: Logger) = {
    // '.' seems to be problematic when specifying the options
    // FIXME I don't know why to replace "_" with ".", but it will lead to unexpected result
    // if "_" is legal such as "starrocks.write.properties.partial_update". just skip to
    // replace parameters for write
    val dottedParams = parameters.map {
      case (k, v) =>
        if (k.startsWith(WriteStarRocksConfig.WRITE_PREFIX)) (k,v)
        else (k.replace('_', '.'), v)
    }

    val preferredTableIdentifier = dottedParams.get(ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER)
      .orElse(dottedParams.get(ConfigurationOptions.TABLE_IDENTIFIER))
    logger.debug(s"preferred Table Identifier is '$preferredTableIdentifier'.")

    // Convert simple parameters into internal properties, and prefix other parameters
    // Convert password parameters from "password" into internal password properties
    // reuse credentials mask method in spark ExternalCatalogUtils#maskCredentials
    val prefixParams = dottedParams.map {
      case (k, v) =>
        if (k.startsWith("starrocks.")) (k, v)
        else ("starrocks." + k, v)
    }

    val replaceParams = prefixParams.map{
      case (ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD, _) =>
        logger.error(s"${ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD} cannot use in StarRocks Datasource.")
        throw new StarRocksException(s"${ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD} cannot use in" +
          s" StarRocks Datasource, use 'password' option to set password.")
      case (ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER, _) =>
        logger.error(s"${ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER} cannot use in StarRocks Datasource.")
        throw new StarRocksException(s"${ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER} cannot use in" +
          s" StarRocks Datasource, use 'user' option to set user.")
      case (ConfigurationOptions.STARROCKS_PASSWORD, v) =>
        (ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD, v)
      case (ConfigurationOptions.STARROCKS_USER, v) =>
        (ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER, v)
      case (k, v) => (k, v)
    }

    // keep the original parameters to be compatible. For example, STARROCKS_PASSWORD and
    // STARROCKS_REQUEST_AUTH_PASSWORD are both valid
    var processedParams = replaceParams ++ prefixParams
    // support both STARROCKS_FENODES and KEY_FE_HTTP
    if (!dottedParams.contains(ConfigurationOptions.STARROCKS_FENODES) && dottedParams.contains(KEY_FE_HTTP)) {
      processedParams = processedParams + (ConfigurationOptions.STARROCKS_FENODES -> dottedParams(KEY_FE_HTTP))
    }
    if (dottedParams.contains(ConfigurationOptions.STARROCKS_FENODES) && !dottedParams.contains(KEY_FE_HTTP)) {
      processedParams = processedParams + (KEY_FE_HTTP -> dottedParams(ConfigurationOptions.STARROCKS_FENODES))
    }

    // Set the preferred resource if it was specified originally
    val finalParams = preferredTableIdentifier match {
      case Some(tableIdentifier) => processedParams + (ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER -> tableIdentifier)
      case None => processedParams
    }

    // validate path is available
    finalParams.getOrElse(ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER,
        throw new StarRocksException("table identifier must be specified for StarRocks table identifier."))

    finalParams
  }
}
