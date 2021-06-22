import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import java.io._
/**
 * UDAF which builds HLL Plus from String Column Values and returns
 * serialized HLL Object encoded as Base64 String
 *
 * Uses stream-lib HLL Plus implementation
 */
class HyperLogLogPlusAgg extends UserDefinedAggregateFunction {
  val hll = new HyperLogLogPlus(14, 25)
  @throws(classOf[IOException])
  def serializeHLL(obj: Object): Array[Byte] = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream(512)
    var out: ObjectOutputStream = null
    try {
      out = new ObjectOutputStream(baos)
      out.writeObject(obj)
    } finally {
      if (out != null) {
        out.close
      }
    }
    return baos.toByteArray
  }

  @throws(classOf[ClassNotFoundException])
  @throws(classOf[IOException])
  def deserializeHLL(bytes: Array[Byte]): HyperLogLogPlus = {
    val bais: ByteArrayInputStream = new ByteArrayInputStream(bytes)
    var in: ObjectInputStream = null
    try {
      in = new ObjectInputStream(bais)
      return in.readObject.asInstanceOf[HyperLogLogPlus]
    } finally {
      if (in != null) {
        in.close
      }
    }
  }
  // input can be any type , but we use String , probably we can make this generic and test
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", StringType) :: Nil)

  // Internal Fields to keep aggregate
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("hllbits", BinaryType) :: Nil
  )

  // output will be Base64 encoded HLL Byte Array
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  // initializing hll buffer with empty HLL
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = serializeHLL(hll)
  }

  // update hll with value from input column and deserialize back to buffer
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    hll.offer(input.getAs[String](0))
    buffer(1) = serializeHLL(hll)
    buffer(0)=hll.cardinality()
    //println(hll.cardinality()+" as "+buffer(0))
    return buffer(0)
  }

  // merge HLLs to buffer
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val mergedHLL = deserializeHLL(buffer1.getAs[Array[Byte]](1))
      .merge(deserializeHLL(buffer2.getAs[Array[Byte]](1)))
    buffer1(1) = serializeHLL(mergedHLL)
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    //println("hhh")
  }

  // Convert serialized HLL from buffer  to Base64
  override def evaluate(buffer: Row): Any = {
    //new String(Base64.getEncoder.encode(buffer.getAs[Array[Byte]](1)))
    buffer.getLong(0)
  }
}
