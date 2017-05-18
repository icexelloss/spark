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

package org.apache.spark.sql.execution.arrow

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.{BaseAllocator, BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.BaseValueVector.{BaseAccessor, BaseMutator}
import org.apache.arrow.vector.file._
import org.apache.arrow.vector.schema.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.stream.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

trait ClosableIterator[T] extends Iterator[T] with AutoCloseable

/**
 * Stream an iter of rows to an output stream in small batches in Arrow Streaming format
 */
// TODO: is BufferAllocator thread safe?
private[sql] class ArrowStreamSerializer(
    schema: StructType,
    allocator: BufferAllocator) {
  // Write to out in Streaming format, this is a blocking call until all data is written
  def dump(out: DataOutputStream, rows: Iterator[InternalRow]): Unit = {
    val recordBatch = ArrowConverters.internalRowIterToArrowBatch(rows, schema, allocator)
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val loader = new VectorLoader(root)
    val writer = new ArrowStreamWriter(root, null, out)
    loader.load(recordBatch)
    writer.start()
    writer.writeBatch()
    writer.close()
  }

  // load data from input stream to an iterator. The iterator allocates memory and must be closed.
  def load(in: DataInputStream): ClosableIterator[UnsafeRow] = {
    val reader = new ArrowStreamReader(in, allocator)
    val root = reader.getVectorSchemaRoot
    // Supports only one batch now
    reader.loadNextBatch()
    new ArrowBackendUnsafeRowIterator(root, schema, root.getRowCount, schema.size)
  }
}


/**
 * The bytes are in arrow file format that containing one batches
 */
private[sql] class ArrowPayload(payload: Array[Byte]) extends Serializable {

  /**
   * Create an ArrowPayload from an ArrowRecordBatch and Spark schema.
   */
  def this(batch: ArrowRecordBatch, schema: StructType, allocator: BufferAllocator) = {
    this(ArrowConverters.batchToByteArray(batch, schema, allocator))
  }

  /**
   * Convert the ArrowPayload to an ArrowRecordBatch.
   */
  def loadBatch(allocator: BufferAllocator): ArrowRecordBatch = {
    ArrowConverters.byteArrayToBatch(payload, allocator)
  }

  /**
   * Get the ArrowPayload as an Array[Byte].
   */
  def toByteArray: Array[Byte] = payload
}

private[sql] object ArrowConverters {

  /**
   * Map a Spark DataType to ArrowType.
   */
  private[arrow] def sparkTypeToArrowType(dataType: DataType): ArrowType = {
    dataType match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ShortType => new ArrowType.Int(8 * ShortType.defaultSize, true)
      case IntegerType => new ArrowType.Int(8 * IntegerType.defaultSize, true)
      case LongType => new ArrowType.Int(8 * LongType.defaultSize, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case ByteType => new ArrowType.Int(8, true)
      case StringType => ArrowType.Utf8.INSTANCE
      case BinaryType => ArrowType.Binary.INSTANCE
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }

  /**
   * Convert a Spark Dataset schema to Arrow schema.
   */
  private[arrow] def schemaToArrowSchema(schema: StructType): Schema = {
    val arrowFields = schema.fields.map { f =>
      new Field(f.name, f.nullable, sparkTypeToArrowType(f.dataType), List.empty[Field].asJava)
    }
    new Schema(arrowFields.toList.asJava)
  }

  /**
   * Maps Iterator from InternalRow to ArrowPayload. Limit ArrowRecordBatch size in ArrowPayload
   * by setting maxRecordsPerBatch or use 0 to fully consume rowIter.
   */
  private[sql] def toPayloadIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Int = 0): Iterator[ArrowPayload] = {
    new Iterator[ArrowPayload] {
      private val _allocator = new RootAllocator(Long.MaxValue)
      private var _nextPayload = if (rowIter.nonEmpty) convert() else null

      override def hasNext: Boolean = _nextPayload != null

      override def next(): ArrowPayload = {
        val obj = _nextPayload
        if (hasNext) {
          if (rowIter.hasNext) {
            _nextPayload = convert()
          } else {
            _allocator.close()
            _nextPayload = null
          }
        }
        obj
      }

      private def convert(): ArrowPayload = {
        val batch = internalRowIterToArrowBatch(rowIter, schema, _allocator, maxRecordsPerBatch)
        new ArrowPayload(batch, schema, _allocator)
      }
    }
  }

  /**
   * Iterate over InternalRows and write to an ArrowRecordBatch, stopping when rowIter is consumed
   * or the number of records in the batch equals maxRecordsInBatch.  If maxRecordsPerBatch is 0,
   * then rowIter will be fully consumed.
   */
  private[sql] def internalRowIterToArrowBatch(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      allocator: BufferAllocator,
      maxRecordsPerBatch: Int = 0): ArrowRecordBatch = {

    val columnWriters = schema.fields.zipWithIndex.map { case (field, ordinal) =>
      ColumnWriter(field.dataType, ordinal, allocator).init()
    }

    val writerLength = columnWriters.length
    var recordsInBatch = 0
    while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || recordsInBatch < maxRecordsPerBatch)) {
      val row = rowIter.next()
      var i = 0
      while (i < writerLength) {
        columnWriters(i).write(row)
        i += 1
      }
      recordsInBatch += 1
    }

    val (fieldNodes, bufferArrays) = columnWriters.map(_.finish()).unzip
    val buffers = bufferArrays.flatten

    val rowLength = if (fieldNodes.nonEmpty) fieldNodes.head.getLength else 0
    val recordBatch = new ArrowRecordBatch(rowLength,
      fieldNodes.toList.asJava, buffers.toList.asJava)

    buffers.foreach(_.release())
    recordBatch
  }

  private[sql] def toUnsafeRowsIter(
      payload: ArrowPayload,
      schema: StructType,
      allocator: BufferAllocator
  ): ArrowBackendUnsafeRowIterator = {
    val bytes = payload.toByteArray
    val inputChannel = new ByteArrayReadableSeekableByteChannel(bytes)
    val reader = new ArrowFileReader(inputChannel, allocator)
    val root = reader.getVectorSchemaRoot
    // ArrowPayLoad contains only one batch.
    assert(reader.getRecordBlocks.size() == 1)
    // println("before loading batch:" + allocator.toString)
    reader.loadNextBatch()
    // println("after loading batch:" + allocator.toString)
    new ArrowBackendUnsafeRowIterator(root, schema, root.getRowCount, schema.size)
  }

  /**
   * Convert an ArrowRecordBatch to a byte array and close batch to release resources. Once closed,
   * the batch can no longer be used.
   */
  private[arrow] def batchToByteArray(
      batch: ArrowRecordBatch,
      schema: StructType,
      allocator: BufferAllocator): Array[Byte] = {
    val arrowSchema = ArrowConverters.schemaToArrowSchema(schema)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val out = new ByteArrayOutputStream()
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))

    // Write a batch to byte stream, ensure the batch, allocator and writer are closed
    Utils.tryWithSafeFinally {
      val loader = new VectorLoader(root)
      loader.load(batch)
      writer.writeBatch()  // writeBatch can throw IOException
    } {
      batch.close()
      root.close()
      writer.close()
    }
    out.toByteArray
  }

  /**
   * Convert a byte array to an ArrowRecordBatch.
   */
  private[arrow] def byteArrayToBatch(
      batchBytes: Array[Byte],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val in = new ByteArrayReadableSeekableByteChannel(batchBytes)
    val reader = new ArrowFileReader(in, allocator)

    // Read a batch from a byte stream, ensure the reader is closed
    Utils.tryWithSafeFinally {
      val root = reader.getVectorSchemaRoot  // throws IOException
      val unloader = new VectorUnloader(root)
      reader.loadNextBatch()  // throws IOException
      unloader.getRecordBatch
    } {
      reader.close()
    }
  }
}

/**
 * Interface for writing InternalRows to Arrow Buffers.
 */
private[arrow] trait ColumnWriter {
  def init(): this.type
  def write(row: InternalRow): Unit

  /**
   * Clear the column writer and return the ArrowFieldNode and ArrowBuf.
   * This should be called only once after all the data is written.
   */
  def finish(): (ArrowFieldNode, Array[ArrowBuf])
}

/**
 * Base class for flat arrow column writer, i.e., column without children.
 */
private[arrow] abstract class PrimitiveColumnWriter(val ordinal: Int)
  extends ColumnWriter with AutoCloseable {

  def getFieldType(dtype: ArrowType): FieldType = FieldType.nullable(dtype)

  def valueVector: BaseDataValueVector
  def valueMutator: BaseMutator

  def setNull(): Unit
  def setValue(row: InternalRow): Unit

  protected var count = 0
  protected var nullCount = 0

  override def init(): this.type = {
    valueVector.allocateNew()
    this
  }

  override def write(row: InternalRow): Unit = {
    if (row.isNullAt(ordinal)) {
      setNull()
      nullCount += 1
    } else {
      setValue(row)
    }
    count += 1
  }

  override def finish(): (ArrowFieldNode, Array[ArrowBuf]) = {
    valueMutator.setValueCount(count)
    val fieldNode = new ArrowFieldNode(count, nullCount)
    val valueBuffers = valueVector.getBuffers(true)
    (fieldNode, valueBuffers)
  }

  override def close(): Unit = valueVector.close()
}

private[arrow] class BooleanColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableBitVector
    = new NullableBitVector("BooleanValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableBitVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, if (row.getBoolean(ordinal)) 1 else 0 )
}

private[arrow] class ShortColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableSmallIntVector
    = new NullableSmallIntVector("ShortValue", getFieldType(dtype: ArrowType), allocator)
  override val valueMutator: NullableSmallIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getShort(ordinal))
}

private[arrow] class IntegerColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableIntVector
    = new NullableIntVector("IntValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getInt(ordinal))
}

private[arrow] class LongColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableBigIntVector
    = new NullableBigIntVector("LongValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableBigIntVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getLong(ordinal))
}

private[arrow] class FloatColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableFloat4Vector
    = new NullableFloat4Vector("FloatValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableFloat4Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getFloat(ordinal))
}

private[arrow] class DoubleColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableFloat8Vector
    = new NullableFloat8Vector("DoubleValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableFloat8Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getDouble(ordinal))
}

private[arrow] class ByteColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableUInt1Vector
    = new NullableUInt1Vector("ByteValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableUInt1Vector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit
    = valueMutator.setSafe(count, row.getByte(ordinal))
}

private[arrow] class UTF8StringColumnWriter(
    dtype: ArrowType,
    ordinal: Int,
    allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("UTF8StringValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    val bytes = row.getUTF8String(ordinal).getBytes
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class BinaryColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableVarBinaryVector
    = new NullableVarBinaryVector("BinaryValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableVarBinaryVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    val bytes = row.getBinary(ordinal)
    valueMutator.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class DateColumnWriter(dtype: ArrowType, ordinal: Int, allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableDateDayVector
    = new NullableDateDayVector("DateValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableDateDayVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    valueMutator.setSafe(count, row.getInt(ordinal))
  }
}

private[arrow] class TimeStampColumnWriter(
    dtype: ArrowType,
    ordinal: Int,
    allocator: BufferAllocator)
  extends PrimitiveColumnWriter(ordinal) {
  override val valueVector: NullableTimeStampMicroVector
    = new NullableTimeStampMicroVector("TimeStampValue", getFieldType(dtype), allocator)
  override val valueMutator: NullableTimeStampMicroVector#Mutator = valueVector.getMutator

  override def setNull(): Unit = valueMutator.setNull(count)
  override def setValue(row: InternalRow): Unit = {
    valueMutator.setSafe(count, row.getLong(ordinal))
  }
}

private[arrow] object ColumnWriter {

  /**
   * Create an Arrow ColumnWriter given the type and ordinal of row.
   */
  def apply(dataType: DataType, ordinal: Int, allocator: BufferAllocator): ColumnWriter = {
    val dtype = ArrowConverters.sparkTypeToArrowType(dataType)
    dataType match {
      case BooleanType => new BooleanColumnWriter(dtype, ordinal, allocator)
      case ShortType => new ShortColumnWriter(dtype, ordinal, allocator)
      case IntegerType => new IntegerColumnWriter(dtype, ordinal, allocator)
      case LongType => new LongColumnWriter(dtype, ordinal, allocator)
      case FloatType => new FloatColumnWriter(dtype, ordinal, allocator)
      case DoubleType => new DoubleColumnWriter(dtype, ordinal, allocator)
      case ByteType => new ByteColumnWriter(dtype, ordinal, allocator)
      case StringType => new UTF8StringColumnWriter(dtype, ordinal, allocator)
      case BinaryType => new BinaryColumnWriter(dtype, ordinal, allocator)
      case DateType => new DateColumnWriter(dtype, ordinal, allocator)
      case TimestampType => new TimeStampColumnWriter(dtype, ordinal, allocator)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }
}

private[sql] trait RowFieldWriter[T <: BaseAccessor] {
  val unsafeRowWriter: UnsafeRowWriter
  val arrowValueAccessor: T
  def write(rowIndex: Int)
}

private[sql] abstract class PrimitiveRowFieldWriter[T <: BaseAccessor](
    val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: T
) extends RowFieldWriter[T] {
  protected def writeValue(rowIndex: Int): Unit
  override def write(rowIndex: Int): Unit = {
    if (arrowValueAccessor.isNull(rowIndex)) {
      unsafeRowWriter.setNullAt(ordinal)
    } else {
      writeValue(rowIndex)
    }
  }
}

private[sql] class BooleanRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableBitVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class ShortRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableSmallIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class IntegerRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class LongRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableBigIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class FloatRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableFloat4Vector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class DoubleRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableFloat8Vector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class ByteRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableTinyIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class UTF8StringRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableVarCharVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, UTF8String.fromBytes(arrowValueAccessor.get(rowIndex)))
  }
}

private[sql] class BinaryRowFieldWriter(
    override val ordinal: Int,
    override val unsafeRowWriter: UnsafeRowWriter,
    override val arrowValueAccessor: NullableVarBinaryVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

private[sql] class ArrowBackendUnsafeRowIterator(
    root: VectorSchemaRoot,
    schema: StructType,
    rowCount: Int,
    columnCount: Int
) extends ClosableIterator[UnsafeRow] {

  val expectedFields: Seq[Field] = ArrowConverters.schemaToArrowSchema(schema).getFields.asScala
  val actualFields: Seq[Field] = root.getSchema.getFields.asScala

  // TODO: Handle nullable field better
  (actualFields zip expectedFields).foreach{ case (f1, f2) =>
    require(f1.getName.equals(f2.getName) && f1.getType.equals(f2.getType),
      s"actual: $f1 expected: $f2")
  }

  private[this] var rowIndex = 0
  private[this] val unsafeRow = new UnsafeRow(columnCount)
  private[this] val unsafeRowBufferHolder = new BufferHolder(unsafeRow, 0)
  private[this] val unsafeRowWriter = new UnsafeRowWriter(unsafeRowBufferHolder, columnCount)
  private[this] val accessors = root.getFieldVectors.asScala.toArray.map(_.getAccessor())
  private[this] val rowFieldWriters = for (i <- 0 until columnCount)
    yield RowFieldWriter(i, unsafeRowWriter, accessors(i), schema(i).dataType)

  override def hasNext: Boolean = rowIndex < rowCount

  override def next(): UnsafeRow = {
    unsafeRowBufferHolder.reset()
    unsafeRowWriter.zeroOutNullBytes()
    var i = 0
    while (i < columnCount) {
      rowFieldWriters(i).write(rowIndex)
      i += 1
    }
    rowIndex += 1
    unsafeRow.setTotalSize(unsafeRowBufferHolder.totalSize)
    unsafeRow
  }

  override def close(): Unit = root.close()
}

private[sql] object RowFieldWriter {
  def apply(
      ordinal: Int,
      unsafeRowWriter: UnsafeRowWriter,
      arrowAccessor: ValueVector.Accessor,
      dataType: DataType):
  RowFieldWriter[_] = {
    dataType match {
      case BooleanType => new BooleanRowFieldWriter(ordinal, unsafeRowWriter,
        arrowAccessor.asInstanceOf[NullableBitVector#Accessor])
      case ShortType => new ShortRowFieldWriter(ordinal, unsafeRowWriter,
        arrowAccessor.asInstanceOf[NullableSmallIntVector#Accessor])
      case IntegerType => new IntegerRowFieldWriter(ordinal, unsafeRowWriter,
        arrowAccessor.asInstanceOf[NullableIntVector#Accessor])
      case LongType => new LongRowFieldWriter(ordinal, unsafeRowWriter,
        arrowAccessor.asInstanceOf[NullableBigIntVector#Accessor])
      case FloatType => new FloatRowFieldWriter(ordinal, unsafeRowWriter,
        arrowAccessor.asInstanceOf[NullableFloat4Vector#Accessor])
      case DoubleType => new DoubleRowFieldWriter(ordinal, unsafeRowWriter,
        arrowAccessor.asInstanceOf[NullableFloat8Vector#Accessor])
      case ByteType => new ByteRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
          .asInstanceOf[NullableTinyIntVector#Accessor])
      case StringType => new UTF8StringRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
          .asInstanceOf[NullableVarCharVector#Accessor])
      case BinaryType => new BinaryRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
          .asInstanceOf[NullableVarBinaryVector#Accessor])
      // TODO: Enable Date and Timestamp type with Arrow 0.3
      // case DateType => new DateColumnWriter(ordinal, allocator)
      // case TimestampType => new TimeStampColumnWriter(ordinal, allocator)
      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }
}
