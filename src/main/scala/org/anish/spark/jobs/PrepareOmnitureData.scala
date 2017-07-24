package org.anish.spark.jobs

import org.anish.spark.RawDataReader
import org.anish.spark.session.MineSessions
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by anish on 19/07/17.
  */
object PrepareOmnitureData extends SparkJob("Prepare Omniture Data") {

  def main(args: Array[String]): Unit = {
    conf = Config.parseArgs(args)
    // SQL Context doesn't support Window functions in v1.6
    import hiveContext.implicits._

    val omnitureLogs = RawDataReader.readOmnitureHive(conf.rawOmnitureHiveTableName)
      .withColumn("ts", unix_timestamp('ts))
      .withColumn("ts", 'ts + 151459200) // Bring things to 2017, because we got old data for demo

    val sessionWindow = Window partitionBy 'sessionid // Window to find session duration
    val omnitureCalculatedSessionDf = MineSessions.mineSessionsFromSource(omnitureLogs) // This adds a session id
      .withColumn("sessionDuration", (max('ts) over sessionWindow) - (min('ts) over sessionWindow)) // Find session duration


    omnitureCalculatedSessionDf
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("date_ts")
      .saveAsTable(conf.processedOmnitureHiveTableName)
  }
}
