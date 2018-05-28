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

package org.apache.spark.sql.execution

import java.util.{HashMap => JHashMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical._

class NullUnsafeProjection extends UnsafeProjection {
  override def apply(row: InternalRow): UnsafeRow = new UnsafeRow()
}

case class AsofJoinExec(
    leftOnExpr: Expression,
    rightOnExpr: Expression,
    leftByExpr: Seq[Expression],
    rightByExpr: Seq[Expression],
    tolerance: Long,
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode {

  override val output: Seq[Attribute] =
    left.output ++ right.output.map(_.withNullability(true))

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override val requiredChildDistribution: Seq[Distribution] = {
    Seq(
      DelayedOverlappedRangeDistribution(leftOnExpr, 0, true),
      DelayedOverlappedRangeDistribution(rightOnExpr, tolerance, false)
    )
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(leftOnExpr.map(SortOrder(_, Ascending)), rightOnExpr.map(SortOrder(_, Ascending)))

  override def outputOrdering: Seq[SortOrder] = Seq(SortOrder(leftOnExpr, Ascending))

  /**
   * Iterates until we are at the last row without going over current key,
   * save the last row.
   */
  @annotation.tailrec
  private def catchUp(
      leftOn: Long,
      rightIter: BufferedIterator[InternalRow],
      rightOnProj: UnsafeProjection,
      rightByProj: UnsafeProjection,
      lastSeen: JHashMap[UnsafeRow, InternalRow]
  ) {
    val nextRight = if (rightIter.hasNext) rightIter.head else null

    if (nextRight != null) {
      val rightOn = rightOnProj(nextRight).getLong(0)
      val rightBy = rightByProj(nextRight)
      if (rightOn <= leftOn) {
        val row = rightIter.next
        // TODO: Can we avoid the copy?
        lastSeen.put(rightBy.copy(), row.copy())
        catchUp(leftOn, rightIter, rightOnProj, rightByProj, lastSeen)
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>

      val rightNullRow = new GenericInternalRow(right.output.length)
      val joinedRow = new JoinedRow()

      val leftOnProj = UnsafeProjection.create(Seq(leftOnExpr), left.output)
      val rightOnProj = UnsafeProjection.create(Seq(rightOnExpr), right.output)
      val leftByProj = if (leftByExpr.isEmpty) {
        new NullUnsafeProjection()
      } else {
        UnsafeProjection.create(leftByExpr, left.output)
      }
      val rightByProj = if (rightByExpr.isEmpty) {
        new NullUnsafeProjection()
      } else {
        UnsafeProjection.create(rightByExpr, right.output)
      }

      val resultProj = UnsafeProjection.create(output, output)
      val bufferedRightIter = rightIter.buffered

      // TODO: Use unsafe map?
      val lastSeen = new JHashMap[UnsafeRow, InternalRow](1024)

      leftIter.map { leftRow =>
        val leftOn = leftOnProj(leftRow).getLong(0)
        catchUp(leftOn, bufferedRightIter, rightOnProj, rightByProj, lastSeen)

        val rightRow = lastSeen.get(leftByProj(leftRow))

        if (rightRow == null) {
          resultProj(joinedRow.withLeft(leftRow).withRight(rightNullRow))
        } else {
          val rightOn = rightOnProj(rightRow).getLong(0)

          if (leftOn - tolerance <= rightOn && rightOn <= leftOn) {
            resultProj(joinedRow.withLeft(leftRow).withRight(rightRow))
          } else {
            resultProj(joinedRow.withLeft(leftRow).withRight(rightNullRow))
          }
        }
      }
    }
  }
}
