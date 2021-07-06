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


class MapAccumulator extends AccumulatorV2[(String, Int), List[(String, Int)]] {

  private val underlyingMap: mutable.HashMap[String, Int] = mutable.HashMap.empty
  override def isZero: Boolean = underlyingMap.isEmpty

  // A flag variable taking boolean value to decide whether to depict the original keys or not
  private var shouldDisplay=false

  // The replacement key in case the original keys must be hidden
  val keyword="***********"

  // Constructor taking value for shouldDisplay variable
  def this(shouldDisplay:Boolean)
  {
    this()
    this.shouldDisplay=shouldDisplay
  }

  override def copy(): AccumulatorV2[(String, Int), List[(String, Int)]] = {
    val newMapAccumulator = new MapAccumulator()
    underlyingMap.foreach(newMapAccumulator.add)
    newMapAccumulator
  }

  override def reset(): Unit = underlyingMap.clear

  def get:Map[String,Int]={
    underlyingMap.toMap
  }

  override def value: List[(String,Int)] = {
    if (shouldDisplay){
      var finalMap=underlyingMap.toList
      finalMap=finalMap.map{ case (x, y) => (keyword, y) }
      finalMap
    }
    else
      underlyingMap.toMap.toList
  }

  override def add(kv: (String, Int)): Unit = {
    val (k, v) = kv
    underlyingMap += k -> (underlyingMap.getOrElse(k, 0) + v)
  }

  override def merge(other: AccumulatorV2[(String, Int), List[(String, Int)]]): Unit =
    other match {
      case map: MapAccumulator =>
        map.get.foreach(this.add)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
}