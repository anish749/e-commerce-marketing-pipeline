package org.anish.spark.session.entities

import org.apache.spark.sql.Row

/**
  * Created by anish on 17/07/17.
  */
case class HitKey(userid: String, ip: String, ts: Long)

object HitKey {
  implicit def orderingByUserIdIpTs[A <: HitKey]: Ordering[A] = {
    Ordering.by(hk => (hk.userid, hk.ip, hk.ts))
  }

  def fromRow(r: Row): HitKey = {
    HitKey(
      r.getAs[String]("userid"),
      r.getAs[String]("ip"),
      r.getAs[Long]("ts")
    )
  }
}