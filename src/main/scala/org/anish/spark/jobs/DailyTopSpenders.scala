package org.anish.spark.jobs

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

/**
  * Created by anish on 21/07/17.
  */
object DailyTopSpenders extends SparkJob("Map sessions with sales data") {

  def main(args: Array[String]): Unit = {
    conf = Config.parseArgs(args)
    import hiveContext.implicits._

    val sessionTable = hiveContext.table(conf.warehouseSessionMappedHiveTableName)
      .filter('date_ts === conf.date_ts)

    val topSpenders = sessionTable
      .groupBy('date_ts, 'userid)
      .agg(sum('UnitPrice * 'Quantity) as "daily_spend")
      .select('date_ts, 'userid, 'daily_spend)
      .sort('daily_spend.desc)
      .limit(conf.topSpendersToAnalyze)

    // Separate out data for daily top buyers
    topSpenders.cache
      .write
      .mode(SaveMode.Append)
      .partitionBy("date_ts")
      .saveAsTable(conf.warehousetopSpendersHiveTableName)

    val omnitureData = hiveContext.table(conf.processedOmnitureHiveTableName)

    val topSpenderUsage = omnitureData.join(topSpenders, omnitureData("userid") === topSpenders("userid") &&
      omnitureData("date_ts") === topSpenders("date_ts")
    )
      .drop(topSpenders("userid"))
      .drop(topSpenders("date_ts"))

    topSpenderUsage
      .write
      .mode(SaveMode.Append)
      .partitionBy("date_ts")
      .saveAsTable(conf.warehouseTopSpenderUsageHiveTableName)

    topSpenders.unpersist()

  }
}
