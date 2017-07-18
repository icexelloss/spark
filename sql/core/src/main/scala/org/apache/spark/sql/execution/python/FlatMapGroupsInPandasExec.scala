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

package org.apache.spark.sql.execution.python

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PandasUdfPythonFunctionType, PythonFunction, PythonRunner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowPayload}
import org.apache.spark.sql.execution.arrow.ArrowConverters.ConcatClosableIterator
import org.apache.spark.sql.types.{StructField, StructType}

case class FlatMapGroupsInPandasExec(
    grouping: Seq[Expression],
    func: Expression,
    override val child: SparkPlan
) extends UnaryExecNode {

  val groupingAttributes: Seq[Attribute] = grouping.map {
    case ne: NamedExpression => ne.toAttribute
  }

  override val output: Seq[Attribute] = func.dataType match {
    case s: StructType => s.map {
      case StructField(name, dataType, nullable, metadata) =>
        AttributeReference(name, dataType, nullable, metadata)()
    }
  }

  private val pandasFunction = func.asInstanceOf[PandasUDF].func

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
    val argOffsets = Array(Array(0))

    inputRDD.mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)

      val allocator = new RootAllocator(Int.MaxValue)
      val context = TaskContext.get()

      val inputIterator = grouped.flatMap { case (_, groupIter) =>
        ArrowConverters.toPayloadIterator(groupIter, child.schema).map(_.asPythonSerializable)
      }

      val outputIterator =
        new PythonRunner(
          chainedFunc,
          bufferSize,
          reuseWorker,
          PandasUdfPythonFunctionType,
          argOffsets
        ).compute(inputIterator, context.partitionId(), context)

      val iters = outputIterator.map { case arrowBytes =>
        val outputArrowPayload = new ArrowPayload(arrowBytes)
        val resultIter = ArrowConverters.toUnsafeRowsIter(outputArrowPayload, schema, allocator)
        resultIter
      }

      val resultIter = new ConcatClosableIterator(iters)

      context.addTaskCompletionListener { _ =>
        // println(s"[${context.taskAttemptId()}] before closing iter: ${allocator.toString}"
        resultIter.close()
        // println(s"[${context.taskAttemptId()}] before closing allocator:
        // ${allocator.toString}")
        // TODO: Allocator.close() throws exception now when
        // the iterator has been properly closed. It
        // appears to be some kind of race condition. Unclear.
        allocator.close()
      }

      resultIter
    }
  }
}
