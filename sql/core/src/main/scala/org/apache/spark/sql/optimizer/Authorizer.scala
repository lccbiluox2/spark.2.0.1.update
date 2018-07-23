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
// scalastyle:off
package org.apache.spark.sql.optimizer

import java.util
import java.util.Properties

import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.BaseRelation

import scala.collection.mutable


object Authorizer extends Rule[LogicalPlan] {

  var relation: BaseRelation = null;

  /**
    * ﻿mvn -Phadoop-2.7 -Dhadoop.version=2.7.4 -DskipTests clean package -pl sql/core
    * spark-submit --class com.dtwave.test.Test --master yarn
    * --conf spark.megrez.userId=4 dev-chance-1.0-SNAPSHOT.jar source
    *
    * cp sql/core/target/spark-sql_2.11-2.0.1.jar ~/opt/third/spark/jars/
    *
    *
    * spark-submit --class com.dtwave.test.Test --master yarn  --conf spark.megrez.implClassName=java.lang.String --conf spark.megrez.enable=1  --conf spark.megrez.userId=210 --conf spark.megrez.tenantId=4 /Users/hulb/project/dtwave/data-etl/dev-chance/target/dev-chance-1.0-SNAPSHOT.jar source
    * spark-submit --class com.dtwave.test.Test --master yarn  --conf spark.megrez.implClassName=org.apache.spark.sql.optimizer.DefaultAuthorizerImpl --conf spark.megrez.enable=1  --conf spark.megrez.userId=210 --conf spark.megrez.tenantId=4 /Users/hulb/project/dtwave/data-etl/dev-chance/target/dev-chance-1.0-SNAPSHOT.jar source
    * spark-submit --class com.dtwave.test.Test --master yarn  --conf spark.megrez.implClassName=org.apache.spark.sql.optimizer.BaseAuthorizeImpl --conf spark.megrez.enable=1  --conf spark.megrez.userId=210 --conf spark.megrez.tenantId=4 /Users/hulb/project/dtwave/data-etl/dev-chance/target/dev-chance-1.0-SNAPSHOT.jar source
    *
    *
    *
    * spark-submit --class com.dtwave.example.Test --master yarn  --conf spark.megrez.implClassName=org.apache.spark.sql.optimizer.BaseAuthorizeImpl --conf spark.megrez.enable=1  --conf spark.megrez.userId=210 --conf spark.megrez.tenantId=4 /Users/hulb/project/dipper/spark-authorizer/example/target/example-spark2.0.jar source
    *
    *
    *
    *
    * spark-submit --class com.dtwave.test.Test --master yarn  --conf spark.megrez.implClassName=org.apache.spark.sql.optimizer.ScalaImpl --conf spark.megrez.enable=1  --conf spark.megrez.userId=210 --conf spark.megrez.tenantId=4 /Users/hulb/project/dtwave/data-etl/dev-chance/target/dev-chance-1.0-SNAPSHOT.jar source
    * *
    *
    * mvn deploy:deploy-file -Dfile=sql/core/target/spark-sql_2.11-2.0.1.jar -DgroupId=com.dtwave.spark -DartifactId=spark-sql_2.11 -Dversion=0.0.1-SNAPSHOT -Durl=http://repo2.dtwave-inc.com/repository/maven-snapshots/ -Dpackaging=jar -DrepositoryId=snapshots
    *
    * @param plan
    * @return
    */

  override def apply(plan: LogicalPlan): LogicalPlan = {

    val douboMap = getMegrezConfig()
//    val userId = SparkSession.getActiveSession.get.conf.get("spark.megrez.userId", "0")
//    val tenantId = SparkSession.getActiveSession.get.conf.get("spark.megrez.tenantId", "0")
//    val workspaceId = SparkSession.getActiveSession.get.conf.get("spark.megrez.workspaceId", "0")
//    val enable = SparkSession.getActiveSession.get.conf.get("spark.megrez.enable", "0")
    val className = SparkSession.getActiveSession.get.conf.get("spark.megrez.implClassName",
      "org.apache.spark.sql.optimizer.BaseAuthorize")

    SparkSession.getActiveSession.get.sqlContext.sessionState

    // 1表示打开鉴权 0 表示关闭 ; 默认为0 关闭
    if ("1".equals(douboMap.get("spark.megrez.enable"))) {
      AuthUtils.auth(plan, douboMap, className)
    }
    //最终把plan返回
    plan
  }


  /**
    * 获取配置中，所有的以spark.megrez.开头的配置，放到map中
    * @return
    */
  def getMegrezConfig(): util.HashMap[String,String] = {
    val douboMap = new util.HashMap[String,String]()
    val runTimeConf: RuntimeConfig = SparkSession.getActiveSession.get.conf
    runTimeConf.getAll.foreach { case (k, v) =>
      if (k.startsWith("spark.megrez.")) {
        douboMap.put(k,v)
      }
    }
    douboMap
  }

}
