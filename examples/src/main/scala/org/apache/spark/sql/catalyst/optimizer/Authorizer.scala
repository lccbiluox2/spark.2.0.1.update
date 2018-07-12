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

//scalastyle:off
package org.apache.spark.sql.catalyst.optimizer

import java.util
import java.util.{List => JList}

import org.apache.hadoop.hive.ql.plan.HiveOperation
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAuthzContext, HiveOperationType, HivePrivilegeObject}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.entity.{Entity, LineageParseResult, MegrezLineageDto, MegrezLineageEdge}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.hive.{DefaultAuthorizerImpl, HivePrivObjsFromPlan}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils


/**
  * Do Hive Authorizing V2, with `Apache Ranger` ranger-hive-plugin well configured,
  * This [[Rule]] provides you with column level fine-gained SQL Standard Authorization.
  * Usage:
  *   1. cp spark-authorizer-<version>.jar $SPARK_HOME/jars
  *   2. install ranger-hive-plugin for spark
  *   3. configure you hive-site.xml and ranger configuration file as shown in [./conf]
  *   4. import org.apache.spark.sql.catalyst.optimizer.Authorizer
  *   5. spark.experimental.extraOptimizations ++= Seq(Authorizer)
  *   6. then suffer for the authorizing pain
  */
object Authorizer {

  /**
    * Visit the [[LogicalPlan]] recursively to get all hive privilege objects, check the privileges
    * using Hive Authorizer V2 which provide sql based authorization and can implements
    * ranger-hive-plugins.
    * If the user is authorized, then the original plan will be returned; otherwise, interrupted by
    * some particular privilege exceptions.
    *
    * @param plan a spark LogicalPlan for verifying privileges
    * @return a plan itself which has gone through the privilege check.
    */
  def check(plan: LogicalPlan, userId: String, tenantId: String): Unit = {

    /**
      * 如果是一个简单的action后的 show（）操作,会导致没有in 和out
      */
    // insert + select 语句这里是 CREATETABLE_AS_SELECT
    val hiveOperationType = toHiveOperationType(plan)
    // val hiveAuthzContext = getHiveAuthzContext(plan)
    SparkSession.getActiveSession match {
      case Some(session) =>
        val db = defaultAuthz.currentDatabase()
        val (in, out) = HivePrivObjsFromPlan.build(plan, db)
        println("currentDatabase:" + db)
        println("input list:" + in.toString)
        println("output list:" + out.toString)
        println("opt type:" + hiveOperationType)
        for (i <- 0 to in.size() - 1) {
          println("input column list:")
          println(in.get(i).getColumns)
        }
        for (i <- 0 to out.size() - 1) {
          println("output column list:")
          println(out.get(i).getColumns)
        }
        println(tenantId)
        println(userId)

        checkPrivileges(userId, tenantId, hiveOperationType, in, out, db)
      case None =>
    }
  }

  def checkPrivileges(
                       userId: String,
                       tenantId: String,
                       hiveOpType: HiveOperationType,
                       inputObjs: JList[HivePrivilegeObject],
                       outputObjs: JList[HivePrivilegeObject],
                       currentDbs: String
                     ): Unit = {

    println("进行血缘解析:");
    val lineageParseResult: LineageParseResult = makeBloodLine(userId, tenantId, hiveOpType, inputObjs, outputObjs, currentDbs)
    println("进行血缘解析:");

  }

  private lazy val defaultAuthz = new DefaultAuthorizerImpl

  /**
    * Mapping of [[LogicalPlan]] -> [[HiveOperation]]
    *
    * @param logicalPlan a spark LogicalPlan
    * @return
    */
  private def logicalPlan2HiveOperation(logicalPlan: LogicalPlan): HiveOperation = {
    logicalPlan match {
      case c: Command => c match {
        case ExplainCommand(child, _, _, _) => logicalPlan2HiveOperation(child)
        case _: LoadDataCommand => HiveOperation.LOAD
        case _: InsertIntoHadoopFsRelationCommand => HiveOperation.CREATETABLE_AS_SELECT
        case _: InsertIntoDataSourceCommand => HiveOperation.QUERY
        case _: CreateDatabaseCommand => HiveOperation.CREATEDATABASE
        case _: DropDatabaseCommand => HiveOperation.DROPDATABASE
        case _: SetDatabaseCommand => HiveOperation.SWITCHDATABASE
        case _: DropTableCommand => HiveOperation.DROPTABLE
        case _: DescribeTableCommand => HiveOperation.DESCTABLE
        case _: DescribeFunctionCommand => HiveOperation.DESCFUNCTION
        case _: AlterTableRecoverPartitionsCommand => HiveOperation.MSCK
        case _: AlterTableRenamePartitionCommand => HiveOperation.ALTERTABLE_RENAMEPART
        case AlterTableRenameCommand(_, _, isView) =>
          if (!isView) HiveOperation.ALTERTABLE_RENAME else HiveOperation.ALTERVIEW_RENAME
        case _: AlterTableDropPartitionCommand => HiveOperation.ALTERTABLE_DROPPARTS
        case _: AlterTableAddPartitionCommand => HiveOperation.ALTERTABLE_ADDPARTS
        case _: AlterTableSetPropertiesCommand
             | _: AlterTableUnsetPropertiesCommand => HiveOperation.ALTERTABLE_PROPERTIES
        case _: AlterTableSerDePropertiesCommand => HiveOperation.ALTERTABLE_SERDEPROPERTIES
        // case _: AnalyzeTableCommand => HiveOperation.ANALYZE_TABLE
        // Hive treat AnalyzeTableCommand as QUERY, obey it.
        case _: AnalyzeTableCommand => HiveOperation.QUERY
        case _: ShowDatabasesCommand => HiveOperation.SHOWDATABASES
        case _: ShowTablesCommand => HiveOperation.SHOWTABLES
        case _: ShowColumnsCommand => HiveOperation.SHOWCOLUMNS
        case _: ShowTablePropertiesCommand => HiveOperation.SHOW_TBLPROPERTIES
        case _: ShowCreateTableCommand => HiveOperation.SHOW_CREATETABLE
        case _: ShowFunctionsCommand => HiveOperation.SHOWFUNCTIONS
        case _: ShowPartitionsCommand => HiveOperation.SHOWPARTITIONS
        case SetCommand(Some((_, None))) | SetCommand(None) => HiveOperation.SHOWCONF
        case _: CreateFunctionCommand => HiveOperation.CREATEFUNCTION
        // Hive don't check privileges for `drop function command`, what about a unverified user
        // try to drop functions.
        // We treat permanent functions as tables for verifying.
        case DropFunctionCommand(_, _, _, false) => HiveOperation.DROPTABLE
        case DropFunctionCommand(_, _, _, true) => HiveOperation.DROPFUNCTION
        case _: CreateViewCommand
             | _: CacheTableCommand
             | _: CreateTempViewUsing => HiveOperation.CREATEVIEW
        case _: UncacheTableCommand => HiveOperation.DROPVIEW
        case _: AlterTableSetLocationCommand => HiveOperation.ALTERTABLE_LOCATION
        case _: CreateTableCommand
             | _: CreateTableCommand
             | _: CreateDataSourceTableCommand => HiveOperation.CREATETABLE
        case _: TruncateTableCommand => HiveOperation.TRUNCATETABLE
        case _: CreateDataSourceTableAsSelectCommand
             | _: CreateHiveTableAsSelectCommand => HiveOperation.CREATETABLE_AS_SELECT
        case _: CreateTableLikeCommand => HiveOperation.CREATETABLE
        case _: AlterDatabasePropertiesCommand => HiveOperation.ALTERDATABASE
        case _: DescribeDatabaseCommand => HiveOperation.DESCDATABASE
        // case _: AlterViewAsCommand => HiveOperation.ALTERVIEW_AS
        case _: AlterViewAsCommand => HiveOperation.QUERY
        case _ =>
          // AddFileCommand
          // AddJarCommand
          HiveOperation.EXPLAIN
      }
      case _: InsertIntoTable => HiveOperation.CREATETABLE_AS_SELECT
      case _ => HiveOperation.QUERY
    }
  }

  private[this] def toHiveOperationType(logicalPlan: LogicalPlan): HiveOperationType = {
    HiveOperationType.valueOf(logicalPlan2HiveOperation(logicalPlan).name())
  }

  /**
    * 判断是字符串为数字
    * @param s    字符串
    * @return     boolean
    */
  def isIntByRegex(s : String) = {
    val pattern = """^(\d+)$""".r
    s match {
      case pattern(_*) => true
      case _ => false
    }
  }


  /**
    * 将输入输出构建成megrez 鉴权的固定格式
    * @param userId
    * @param tenantId
    * @param hiveOpType
    * @param inputObjs
    * @param outputObjs
    * @param currentDbs
    * @return
    */
  def makeBloodLine(
                     userId: String,
                     tenantId: String,
                     hiveOpType: HiveOperationType,
                     inputObjs: JList[HivePrivilegeObject],
                     outputObjs: JList[HivePrivilegeObject],
                     currentDbs: String): LineageParseResult = {

    val lineageParseResult: LineageParseResult = new LineageParseResult()
    lineageParseResult.setOptType(hiveOpType.name())

    // 下面两句可能会抛出类型转换异常
    if(userId != null && isIntByRegex(userId) ){
      var userIdLong:Long = userId.toLong
      lineageParseResult.setUserId(userIdLong)
    }

    if(tenantId != null && isIntByRegex(tenantId) ){
      var tenantIdLong:Long = tenantId.toLong
      lineageParseResult.setTenantId(tenantIdLong)
    }

    // 构建输入列表
    val inputList = new util.ArrayList[Entity]
    // 输出列表
    val outputList = new util.ArrayList[Entity]
    // 所有的字段集合，格式：数据库名.表名.字段名
    val fieldList = new util.ArrayList[String]
    val inFieldList = new util.ArrayList[String]
    val outFieldList = new util.ArrayList[String]
    // 函数列表
    val functionList = new util.ArrayList[String]

    // 当前数据库名称
    var currentDbName: String = null

    val itIn = inputObjs.iterator()
    while (itIn.hasNext) {
      val hiveObj: HivePrivilegeObject = itIn.next()
      val inDbName: String = hiveObj.getDbname
      val inTabName: String = hiveObj.getObjectName
      val inType: String = hiveObj.getType.name()
      val inColumns:JList[String] = hiveObj.getColumns
      if (inDbName != null) {
        currentDbName = inDbName
      }
      // 构建输入列表
      val entity: Entity = new Entity(inDbName, inTabName, inType, null)
      inputList.add(entity)
      if (inColumns != null) {
        val inColumnsList: List[String] = inColumns.asScala.toList
        inColumnsList.foreach(column => {
          val lastColumn = inDbName + "." + inTabName + "." + column
          inFieldList.add(lastColumn)
        })
      }
    }

    lineageParseResult.setInputList(inputList)

    val itOut = outputObjs.iterator()
    while (itOut.hasNext) {
      val hiveObj: HivePrivilegeObject = itOut.next()
      val outDbName: String = hiveObj.getDbname
      val outTabName: String = hiveObj.getObjectName
      val outType: String = hiveObj.getType.name()
      val outColumns:JList[String] = hiveObj.getColumns
      // 构建输入列表
      val entity: Entity = new Entity(outDbName, outTabName, outType, null)
      outputList.add(entity)
      if (outColumns != null) {
        val inColumnsList: List[String] = outColumns.asScala.toList
        inColumnsList.foreach(column => {
          val lastColumn = outDbName + "." + outTabName + "." + column
          outFieldList.add(lastColumn)
        })
      }
    }

    // 设置输出列表
    lineageParseResult.setOutputList(outputList)
    // 设置输入列表
    fieldList.addAll(inFieldList)
    fieldList.addAll(outFieldList)
    lineageParseResult.setFieldList(fieldList)

    // 血缘关系
    val megrezLineageDto: MegrezLineageDto = new MegrezLineageDto()
    megrezLineageDto.setDatabase(currentDbName)
    // key是输出字段 value是输入字段 一个输出字段对应多个输入字段
    val mapFieldList = new util.HashMap[String, util.ArrayList[String]]()
    if (inFieldList != null && inFieldList.size() > 0 && outFieldList != null && outFieldList.size() > 0) {
      // 如果输入字段数小于输出字段数
      if (inFieldList.size() < outFieldList.size()) {
        for (i <- 0 until outFieldList.size()) {
          var valueList = mapFieldList.get(outFieldList.get(i))
          if (valueList == null) {
            valueList = new util.ArrayList[String]
          }
          try {
            valueList.add(inFieldList.get(i))
          } catch {
            case e: IndexOutOfBoundsException => {
              valueList.add(MegrezLineageEdge.unknowField)
            }
          }
          mapFieldList.put(outFieldList.get(i), valueList)
        }
      } else if (inFieldList.size() > outFieldList.size()) {
        // 如果输入字段数大于输出字段数
        var j: Int = 0
        for (i <- 0 until fieldList.size()) {
          if (i % outFieldList.size() == 0) {
            j = 0
          }
          var valueList = mapFieldList.get(outFieldList.get(j))
          if (valueList == null) {
            valueList = new util.ArrayList[String]
          }
          valueList.add(fieldList.get(i))
          mapFieldList.put(outFieldList.get(j), valueList)
          j = j + 1
        }
      }
    }

    // 生成血缘关系对象列表
    val edgesList = new util.ArrayList[MegrezLineageEdge]
    for (key <- mapFieldList.keySet().toArray) {
      val edges = new MegrezLineageEdge
      val targets = new util.ArrayList[String]
      val targetFields: String = key.toString
      targets.add(targetFields)
      edges.setTargets(targets)
      val sourceFiledsList: util.ArrayList[String] = mapFieldList.get(key)
      edges.setSources(sourceFiledsList)
      edgesList.add(edges)
    }

    megrezLineageDto.setEdges(edgesList)
    lineageParseResult.setMegrezLineageDto(megrezLineageDto)

    println(lineageParseResult.toString)
    lineageParseResult

  }


}