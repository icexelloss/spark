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

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.catalyst.plans.logical.{AsofJoin, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.{DelayedOverlappedRangePartitioning, DelayedRange}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{AsofJoinExec, BroadcastAsofJoinExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.internal.SQLConf

// These are codes that can be added via experimental methods
// The actual rules don't need to be in this file. Keep them here for now
// for convenience.

object AsofJoinStrategy extends Strategy {

  private def canBroadcastByHints(left: LogicalPlan, right: LogicalPlan)
  : Boolean = {
    left.stats.hints.broadcast || right.stats.hints.broadcast
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case AsofJoin(left, right, leftOn, rightOn, leftBy, rightBy, tolerance)
      if canBroadcastByHints(left, right) =>
      val buildSide = if (left.stats.hints.broadcast) {
        BuildLeft
      } else {
        BuildRight
      }
      BroadcastAsofJoinExec(
        buildSide,
        leftOn,
        rightOn,
        leftBy, rightBy, tolerance, planLater(left), planLater(right)) :: Nil

    case AsofJoin(left, right, leftOn, rightOn, leftBy, rightBy, tolerance) =>
      AsofJoinExec(
          leftOn,
          rightOn,
          leftBy, rightBy, tolerance, planLater(left), planLater(right)) :: Nil

    case _ => Nil
  }
}

/**
 * This must run after ensure requirements. This is not great but I don't know another way to
 * do this, unless we modify ensure requirements.
 *
 * Currently this mutate the state of partitioning (by setting the delayed range object) so
 * it's not great. We might need to make partitioning immutable and copy nodes with new
 * partitioning.
 *
 */
object EnsureRange extends Rule[SparkPlan] {

  private def ensureChildrenRange(operator: SparkPlan): SparkPlan = operator match {
    case asof: AsofJoinExec =>
      // This code assumes EnsureRequirement will set the left and right partitioning
      // properly
      val leftPartitioning =
        asof.left.outputPartitioning.asInstanceOf[DelayedOverlappedRangePartitioning]
      val rightPartitioning =
        asof.right.outputPartitioning.asInstanceOf[DelayedOverlappedRangePartitioning]

      if (leftPartitioning.delayedRange == null) {
        val delayedRange = new DelayedRange()
        leftPartitioning.setDelayedRange(delayedRange)
        rightPartitioning.setDelayedRange(delayedRange)
      } else {
        rightPartitioning.setDelayedRange(leftPartitioning.delayedRange)
      }
    asof
  }

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: AsofJoinExec => ensureChildrenRange(operator)
  }
}


/**
 * :: Experimental ::
 * Holder for experimental methods for the bravest. We make NO guarantee about the stability
 * regarding binary compatibility and source compatibility of methods here.
 *
 * {{{
 *   spark.experimental.extraStrategies += ...
 * }}}
 *
 * @since 1.3.0
 */
@Experimental
@InterfaceStability.Unstable
class ExperimentalMethods private[sql]() {

  /**
   * Allows extra strategies to be injected into the query planner at runtime.  Note this API
   * should be considered experimental and is not intended to be stable across releases.
   *
   * @since 1.3.0
   */
  @volatile var extraStrategies: Seq[Strategy] = Seq(AsofJoinStrategy)

  @volatile var extraOptimizations: Seq[Rule[LogicalPlan]] = Nil

  @volatile var extraPreparations: Seq[Rule[SparkPlan]] = Seq(EnsureRange)

  override def clone(): ExperimentalMethods = {
    val result = new ExperimentalMethods
    result.extraStrategies = extraStrategies
    result.extraOptimizations = extraOptimizations
    result.extraPreparations = extraPreparations
    result
  }
}
