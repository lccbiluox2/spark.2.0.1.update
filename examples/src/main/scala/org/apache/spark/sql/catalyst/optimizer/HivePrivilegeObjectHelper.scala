/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import java.util.{List => JList}

import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.{HivePrivObjectActionType, HivePrivilegeObjectType}

/**
  * Helper class for initializing [[HivePrivilegeObject]] with more Constructors.
  */
private[sql] object HivePrivilegeObjectHelper {
  def apply(
             `type`: HivePrivilegeObjectType,
             dbname: String,
             objectName: String,
             partKeys: JList[String],
             columns: JList[String],
             actionType: HivePrivObjectActionType,
             commandParams: JList[String]): HivePrivilegeObject = {
    new HivePrivilegeObject(
      `type`, dbname, objectName, partKeys, columns, actionType, commandParams)
  }

  def apply(
             `type`: HivePrivilegeObjectType,
             dbname: String,
             objectName: String,
             partKeys: JList[String],
             columns: JList[String],
             commandParams: JList[String]): HivePrivilegeObject = {
    apply(
      `type`, dbname, objectName, partKeys, columns, HivePrivObjectActionType.OTHER, commandParams)
  }

  def apply(
             `type`: HivePrivilegeObjectType,
             dbname: String,
             objectName: String,
             partKeys: JList[String],
             columns: JList[String]): HivePrivilegeObject = {
    apply(
      `type`, dbname, objectName, partKeys, columns, HivePrivObjectActionType.OTHER, null)
  }

  def apply(
             `type`: HivePrivilegeObjectType,
             dbname: String,
             objectName: String,
             actionType: HivePrivObjectActionType): HivePrivilegeObject = {
    apply(`type`, dbname, objectName, null, null, actionType, null)
  }

  def apply(
             `type`: HivePrivilegeObjectType,
             dbname: String,
             objectName: String): HivePrivilegeObject = {
    apply(`type`, dbname, objectName, HivePrivObjectActionType.OTHER)
  }
}
