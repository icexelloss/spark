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
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonFunction, PythonRunner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowPayload}
import org.apache.spark.sql.types.{StructField, StructType}

case class MapPartitionsInPandasExec(
    func: PythonFunction,
    outputSchema: StructType,
    override val child: SparkPlan
) extends UnaryExecNode {
  override lazy val schema: StructType = outputSchema
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    // Don't need copy here because internalRowIterToPayload is `read and forget`
    val inputRDD = child.execute()
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val chainedFunc = Seq(ChainedPythonFunctions(Seq(func)))
    val argOffsets = Array(Array(0))

    inputRDD.mapPartitionsInternal { iter =>

      val inputArrowPayload = ArrowConverters.toPayloadIterator(iter, child.schema)
      val inputIterator = inputArrowPayload.map(_.batchBytes)
      // TODO: This should be probably created from a global root allocator for the jvm
      val allocator = new RootAllocator(Int.MaxValue)
      val context = TaskContext.get()
      // TODO: Not sure if this is enough to clean up memory
      context.addTaskCompletionListener( _ => allocator.close())

      val outputIterator =
        new PythonRunner(chainedFunc, bufferSize, reuseWorker, true, argOffsets)
            .compute(inputIterator, context.partitionId(), context)

      val outputArrowBytes = outputIterator.next()
      val outputArrowPayload = new ArrowPayload(outputArrowBytes)
      ArrowConverters.toUnsafeRowsIter(Iterator(outputArrowPayload), schema, allocator)
    }
  }

  override def output: Seq[Attribute] = schema.map {
    case StructField(name, dataType, nullable, metadata)
    => AttributeReference(name, dataType, nullable, metadata)()
  }
}
