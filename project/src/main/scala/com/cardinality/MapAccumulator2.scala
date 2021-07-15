package com.cardinality
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
 * MapAccumulator defined to compute the exact count of distinct elements present in the
 * dataset. It works just like a map and maintains a record for every item and the count
 * of how many times that item has appeared. It takes the string value as input and outputs
 * a map depicting which item has appeared how many times. The accumulator also provides the
 * functionality to hide the keys as well if needed.
 */


class MapAccumulator2 extends AccumulatorV2[(String, Int), Int]{

  private val underlyingMap: mutable.HashMap[String, Int] = mutable.HashMap.empty
  override def isZero: Boolean = underlyingMap.isEmpty

  // A flag variable taking boolean value to decide whether to depict the original keys or not
  // private var shouldDisplay:Boolean=true

  // The replacement key in case the original keys must be hidden
  val keyword="***********"

  override def copy(): AccumulatorV2[(String, Int), Int] = {
    val newMapAccumulator = new MapAccumulator2()
    underlyingMap.foreach(newMapAccumulator.add)
    newMapAccumulator
  }

  override def reset(): Unit = underlyingMap.clear

  def get:Map[String,Int]={
    underlyingMap.toMap
  }

  override def value: Int = {
      underlyingMap.toMap.size
  }
  override def add(kv: (String, Int)): Unit = {
    val (k, v) = kv
    underlyingMap += k -> (underlyingMap.getOrElse(k, 0) + v)
  }

  override def merge(other: AccumulatorV2[(String, Int), Int]): Unit =
    other match {
      case map: MapAccumulator2 =>
        map.get.foreach(this.add)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
}