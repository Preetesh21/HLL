import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
class UDAF_Calculate extends UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) :: Nil
  )

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
  }
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}
