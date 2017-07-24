package org.anish.spark.jobs

import org.anish.spark.session.SessionSalesMapper
import org.apache.spark.sql.SaveMode

/**
  * Created by anish on 21/07/17.
  */
object MapSessionSales extends SparkJob("Map sessions with sales data") {

  def main(args: Array[String]): Unit = {
    conf = Config.parseArgs(args)
    import hiveContext.implicits._

    val salesData = hiveContext.table(conf.processedSalesDataHiveTableName)
    val omnitureData = hiveContext.table(conf.processedOmnitureHiveTableName)
      .filter('date_ts === conf.date_ts)

    val mappedData = SessionSalesMapper.mapSalesToSessions(omnitureData, salesData)

    mappedData
      .write
      .mode(SaveMode.Append)
      .partitionBy("date_ts")
      .saveAsTable(conf.warehouseSessionMappedHiveTableName)
  }
}
