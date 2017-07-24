package org.anish.spark.session

import org.anish.spark.session.entities.HitKey
import org.apache.spark.Partitioner

/**
  * Created by anish on 17/07/17.
  */
class HitKeyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[HitKey]
    nonNegativeMod(k.userid.hashCode(), numPartitions)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}