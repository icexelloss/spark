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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, GenericInternalRow, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

/**
 * An alternative physical plan for asof join to use broadcast.
 */
case class BroadcastAsOfJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftOn: Expression,
    rightOn: Expression,
    leftBy: Expression,
    rightBy: Expression,
    tolerance: Long,
    exactMatches: Boolean) extends BinaryExecNode {

  override def output: Seq[Attribute] = left.output ++ right.output.map(_.withNullability((true)))

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil

  override def outputOrdering: Seq[SortOrder] = left.outputOrdering

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq(leftBy, leftOn).map(SortOrder(_, Ascending)) :: Seq() :: Nil
  }

  private def rightNullRow: GenericInternalRow = new GenericInternalRow(right.output.length)
  private def keyOrdering: Ordering[InternalRow] =
    newNaturalAscendingOrdering(leftBy.map(_.dataType))
    private def rightOnOrdering: Ordering[InternalRow] =
    newOrdering(Seq(SortOrder(rightOn, Ascending)), right.output)

  override protected def doExecute() = {
    val rightBroadcast = right.executeBroadcast[Array[InternalRow]]()

    // Zip the left and right plans to group by key.
    left.execute().mapPartitionsInternal { leftIter =>
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)
      // There is no guarantee that right rows are sorted, so we sort it here to be safe.
      // Left rows are sorted because of requiredChildOrdering.
      val rightIter = rightBroadcast.value.sorted(rightOnOrdering).toIterator

      val scanner = new AsofJoinScanner(
        leftIter,
        rightIter,
        leftOn,
        rightOn,
        leftBy,
        rightBy,
        left.output,
        right.output
      )

      new AsofJoinIterator(
        scanner, resultProj, tolerance, exactMatches, keyOrdering, rightNullRow).toScala
    }
  }
}
