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

package org.apache.spark.sql

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.existentials
import org.apache.spark.TestUtils.{assertNotSpilled, assertSpilled}
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Ascending, PythonUDF, SortOrder}
import org.apache.spark.sql.execution.{BinaryExecNode, SortExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.functions.asofJoin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class AsofJoinSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  private lazy val df1 = spark.createDataFrame(Seq(
    (100, 1, 5.0, 3.0),
    (200, 2, 10.0, 4.0),
    (300, 1, 15.0, 6.0),
    (300, 2, 10.0, 5.0),
    (400, 2, 8.0, 7.0)
  )).toDF("time", "id", "v1", "w1")

  private lazy val df2 = spark.createDataFrame(Seq(
    (90, 1, 20.0, 1.0),
    (185, 2, 30.0, 2.0),
    (250, 1, 40.0, 3.0),
    (250, 2, 40.0, 3.0),
    (300, 1, 30.0, 5.0)
  )).toDF("time", "id", "v2", "w2")

  private lazy val df3 = spark.createDataFrame(Seq(
    (90, 1, 20.0, 1.0),
    (185, 2, 30.0, 2.0),
    (250, 1, 40.0, 3.0),
    (300, 1, 50.0, 2.0),
    (400, 1, 60.0, 7.0)
  )).toDF("time", "id", "v3", "w3")

  test("Asof Join 2 with flatmap groups in pandas") {
    import org.apache.spark.sql.functions.{asofJoin, sum, lit, array, explode}

    val df1 = spark.range(0, 1000, 3).toDF("time")
    val df2 = spark.range(1, 1000, 3).toDF("time")
    val tolerance = 10
    val context = (0L, 1000L)

    val udf = PythonUDF("foo", null, StructType(Seq(StructField("time", LongType))),
      Seq(df1("time").expr),
      evalType = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF, true)

    val result = asofJoin(df1, df2, "time", null, tolerance)
      .groupBy(df1("time")).flatMapGroupsInPandas(udf)

    result.explain(true)
  }

  // TODO: Make this work
  test("Asof Join 2 unsorted") {
    import org.apache.spark.sql.functions.{asofJoin, sum, lit, array, explode, col, desc}

    val df1 = spark.range(0, 1000, 3).toDF("time").repartition(47)
    val df2 = spark.range(1, 1000, 3).toDF("time").repartition(47)

    df1.show()
    val tolerance = 10
    val context = (0L, 1000L)

    df1.sparkSession.conf.set("spark.sql.codegen.wholeStage", "false")

    val result = asofJoin(df1, df2, "time", null, tolerance)
    result.explain(true)

    result.show()

    // println(result.queryExecution.executedPlan.execute().first())
  }

  test("Asof join 2") {
    import org.apache.spark.sql.functions.asofJoin

    val df1 = spark.range(0, 1000, 3).toDF("time")
    val df2 = spark.range(1, 1000, 3).toDF("time")
    val tolerance = 10

    val result = asofJoin(df1, df2, "time", null, tolerance)

    result.explain(true)

    result.show(1000)
  }

  test("Asof join 2 with key") {
    val result = asofJoin(df1, df2, "time", "id", tolerance = 100)

    result.explain(true)

    result.show()
  }

  test("Asof Join 3") {
    import org.apache.spark.sql.functions.{asofJoin, lit, array, explode}

    val df1 = spark.range(0, 1000, 3).toDF("time")
    val df2 = spark.range(1, 1000, 3).toDF("time")
    val df3 = spark.range(2, 1000, 3).toDF("time").withColumn("v2", lit(2))
    val tolerance = 10

    val result = asofJoin(
      asofJoin(df1, df2, "time", null, tolerance)
        .withColumn("v1", lit(1)),
      df3,
      "time",
      null,
      tolerance)

    result.explain(true)

    result.show(1000)
  }

  test("column prune") {
    // This works automatically!
    import org.apache.spark.sql.functions.{asofJoin, lit, sum, array, explode}

    val tolerance = 20

     val result = asofJoin(df1, df2, "time", null, tolerance).agg(sum(df2("v2")))
    // val result2 = df1.join(df2, "time").agg(sum(df2("v2")))

    result.explain(true)
  }

  test("count") {
    import org.apache.spark.sql.functions.{asofJoin, lit, sum, array, explode}

    val tolerance = 20

    val result = asofJoin(df1, df2, "time", null, tolerance)

    result.groupBy().count().explain(true)
  }

  test("right broadcast") {
    import org.apache.spark.sql.functions.{asofJoin, broadcast}

    val df1 = spark.range(0, 1000, 3).toDF("time")
    val df2 = spark.range(1, 1000, 3).toDF("time")
    val tolerance = 10

    val result = asofJoin(df1, broadcast(df2), "time", null, tolerance)

    result.explain(true)

    result.show(1000)
  }

  test("left broadcast (not working)") {
    import org.apache.spark.sql.functions.{asofJoin, broadcast}

    val df1 = spark.range(0, 1000, 3).toDF("time")
    val df2 = spark.range(1, 1000, 3).toDF("time")
    val tolerance = 10
    val context = (0L, 1000L)

    val result = asofJoin(broadcast(df1), df2, "time", null, tolerance)

    result.explain(true)

    result.show(1000)
  }

}
