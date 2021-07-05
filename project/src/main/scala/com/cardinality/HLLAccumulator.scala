package com.cardinality

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.util.AccumulatorV2

class HLLAccumulator[T](precisionValue: Int = 12) extends AccumulatorV2[String, Long] with Serializable {
  require(precisionValue>=4 && precisionValue<=32, "precision value must be between 4 and 32")

  private def instance(): HyperLogLogPlus = new HyperLogLogPlus(precisionValue, 0)

  private var hll: HyperLogLogPlus = instance()

  override def isZero: Boolean = {
   // An [[AccumulatorV2 accumulator]] for counting unique elements using a com.important.HyperLogLog
    hll.cardinality() == 0
  }

  override def copyAndReset(): HLLAccumulator[T] = new HLLAccumulator[T](precisionValue)

  override def copy(): HLLAccumulator[T] = {
    val newAcc = new HLLAccumulator[T](precisionValue)
    newAcc.hll.addAll(hll)
    newAcc
  }

  override def reset(): Unit = {
    hll = instance
  }

  override def add(v: String): Unit = hll.offer(v)

  override def merge(other: AccumulatorV2[String, Long]): Unit = other match {
    case otherHllAcc: HLLAccumulator[T] => hll.addAll(otherHllAcc.hll)
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Long = hll.cardinality()
}