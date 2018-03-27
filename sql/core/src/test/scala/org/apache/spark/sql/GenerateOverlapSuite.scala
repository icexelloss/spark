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

import com.google.common.collect.{RangeMap, TreeRangeMap, Range => GRange}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Overlap}
import org.apache.spark.sql.catalyst.plans.physical.DelayedRange
import org.apache.spark.sql.execution.{GenerateExec, LogicalRDD}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.IntegerType

class GenerateOverlapSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  test("Generate simple") {
    import org.apache.spark.sql.functions.{array, lit, explode, col}

    val df1 = spark.range(0, 10, 1).toDF("time").withColumn("v", col("time"))

    val range = new DelayedRange()
    range.setRange(IndexedSeq(GRange.closedOpen(0, 5), GRange.closedOpen(5, 10)))

    val df2 = df1.withColumn("overlap", Column(Overlap(df1("time").expr, 3, range)))

    df2.show()

  }

}
