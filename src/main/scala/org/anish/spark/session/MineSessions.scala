package org.anish.spark.session

import org.anish.spark.session.entities.HitKey
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by anish on 17/07/17.
  */
object MineSessions {

  val PARTITIONS_TO_USE = 15
  val MAX_SESSION_IDLE_TIME = 30 * 60

  /**
    * Mine sessions from the records, by grouping logs from same user and ip address within the
    * MAX_SESSION_IDLE_TIME into one session. This merely adds a session id column, and doesn't do
    * any aggregate on top of the data.
    *
    * @param source
    * @param hiveContext
    * @return
    */
  def mineSessionsFromSource(source: DataFrame)
                            (implicit hiveContext: HiveContext): DataFrame = {
    val kvRdd = source
      .rdd
      .keyBy(HitKey.fromRow) // for partitioning
      .repartitionAndSortWithinPartitions(new HitKeyPartitioner(PARTITIONS_TO_USE)) // sort by keys within each partition

    val sessionedRdd = kvRdd.mapPartitions(iter => {
      var previousHit = (HitKey("", "", 0),
        "") // place holder for session id

      for (currentHit <- iter) yield {
        val sessionId =
          if (currentHit._1.userid == previousHit._1.userid &&
            currentHit._1.ip == previousHit._1.ip &&
            currentHit._1.ts - previousHit._1.ts < MAX_SESSION_IDLE_TIME)
            previousHit._2
          else java.util.UUID.randomUUID.toString

        previousHit = (currentHit._1, sessionId) // store previous state in a variable
        Row.fromSeq(currentHit._2.toSeq :+ sessionId)
      }
    })

    hiveContext.createDataFrame(sessionedRdd,
      StructType(source.schema.fields
        :+ StructField("sessionid", StringType)))
  }

  /**
    * Using Data Frames for secondary sort without using Custom Partitioner (Spark v1.6 onwards)
    *
    * @param source
    * @param hiveContext
    * @return
    */
  def mineSessionsFromSource_df(source: DataFrame)(implicit hiveContext: HiveContext): DataFrame = {
    val sessionedRdd = source.repartition(PARTITIONS_TO_USE, col("userid"))
      .sortWithinPartitions("userid", "ip", "ts")
      .mapPartitions(iter => {
        var previousHit = (HitKey("", "", 0),
          "") // place holder for session id

        for (currentHitRow <- iter) yield {
          val currentHit = HitKey.fromRow(currentHitRow)
          val sessionId =
            if (currentHit.userid == previousHit._1.userid &&
              currentHit.ip == previousHit._1.ip &&
              currentHit.ts - previousHit._1.ts < MAX_SESSION_IDLE_TIME)
              previousHit._2
            else java.util.UUID.randomUUID.toString

          previousHit = (currentHit, sessionId) // store previous state in a variable
          Row.fromSeq(currentHitRow.toSeq :+ sessionId)
        }
      })

    hiveContext.createDataFrame(sessionedRdd,
      StructType(source.schema.fields
        :+ StructField("sessionid", StringType)))
  }

  def printPairRDD(rdd: RDD[(HitKey, Row)]) = {
    rdd.foreachPartition(iter => {
      val list = iter.toList
      val partitionKey = list.headOption.getOrElse((-1, None))._1
      val headValue = list.map(_._2).headOption.getOrElse(None)
      //      println(s"partitionKey is $partitionKey, and value is $hashSet")
      println(s"partitionKey is $partitionKey, length is ${list.length} and head value is $headValue")
    })
  }

}
