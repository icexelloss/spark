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
package org.apache.spark.sql.catalyst.plans.logical

import scala.concurrent.duration.Duration

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.types._

object AsofJoin {
  def apply(left: LogicalPlan, right: LogicalPlan, leftOn: Expression, rightOn: Expression,
            leftBy: Expression, rightBy: Expression, tolerance: String,
            allowExactMatches: Boolean): AsofJoin = {
    val duration = if (Duration(tolerance).isFinite) {
      Duration(tolerance).toMillis
    } else {
      Long.MaxValue
    }

    if (leftOn.dataType != TimestampType) {
      throw new AnalysisException("cannot resolve due to data type mismatch: " +
        s"${leftOn.dataType} should be a TimestampType")
    } else if (rightOn.dataType != TimestampType) {
      throw new AnalysisException("cannot resolve due to data type mismatch: " +
        s"${rightOn.dataType} should be a TimestampType")
    }

    if (leftBy.dataType != rightBy.dataType) {
      throw new AnalysisException("cannot resolve due to data type mismatch: " +
        s"${leftBy.dataType} differs from ${rightBy.dataType} in 'by'")
    }

    new AsofJoin(left, right, leftOn, rightOn, leftBy, rightBy, duration, allowExactMatches)
  }
}

case class AsofJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    leftOn: Expression,
    rightOn: Expression,
    leftBy: Expression,
    rightBy: Expression,
    tolerance: Long,
    allowExactMatches: Boolean)
  extends BinaryNode {

  // TODO polymorphic keys
  override def output: Seq[Attribute] = left.output ++ right.output.map(_.withNullability(true))

  def duplicateResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty
}
