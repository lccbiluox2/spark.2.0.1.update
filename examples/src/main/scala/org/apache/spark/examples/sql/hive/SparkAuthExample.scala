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
package org.apache.spark.examples.sql.hive

// $example on:spark_hive$
import org.apache.spark.sql.{Row, SparkSession}
// $example off:spark_hive$

object SparkAuthExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .config("spark.megrez.userId", 10)
      .config("spark.megrez.tenantId", 20)
      .config("spark.megrez.enable", 1)
      .config("spark.megrez.implClassName", "org.apache.spark.sql.optimizer.BaseAuthorizeImpl")
      .master("local")
      .getOrCreate()

    import spark.sql
    // Queries are expressed in HiveQL
    sql("insert into sink select source_key,source_value from source union select * from mytable")

    sql("insert into sink select source_key,source_value from source")

    spark.stop()
  }
}
