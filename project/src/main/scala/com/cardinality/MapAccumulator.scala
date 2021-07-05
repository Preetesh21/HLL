package com.cardinality
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

class MapAccumulator extends AccumulatorV2[(String, Int), List[(String, Int)]] {

  private val underlyingMap: mutable.HashMap[String, Int] = mutable.HashMap.empty
  override def isZero: Boolean = underlyingMap.isEmpty

  private var redstead=false

  val keyword="***********"

  def this(redstead:Boolean)
  {
    this()
    this.redstead=redstead
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
    if (redstead){
      var m=underlyingMap.toList
      m=m.map{ case (x, y) => (keyword, y) }
      m
      //underlyingMap.toMap.zipWithIndex.map { case ((key, value),index) => keyword_index -> value }
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