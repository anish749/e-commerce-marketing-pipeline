package org.anish.spark

import org.anish.spark.jobs.Config
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by anish on 18/07/17.
  */
object RawDataReader {
  def readOmniture(pathToFile: String)
                  (implicit sqlContext: SQLContext): DataFrame =
    sqlContext
      .read
      .format("csv")
      .option("delimiter", "\t")
      .load(pathToFile)
      .withColumnRenamed("C1", "ts")
      .withColumnRenamed("C7", "ip")
      .withColumnRenamed("C12", "url")
      .withColumnRenamed("C13", "userid")
      .withColumnRenamed("C49", "city")
      .withColumnRenamed("C50", "country")
      .withColumnRenamed("C52", "state")

  def readOmnitureHive(hivetableName: String)
                      (implicit hiveContext: HiveContext, config: Config): DataFrame = {
    import hiveContext.implicits._
    hiveContext.table(hivetableName)
      .filter('date_ts === config.date_ts)
  }

  def readSales(rawSalesDataSource: String)
               (implicit sqlContext: SQLContext): DataFrame = sqlContext
    .read
    .format("csv")
    .option("header", "true")
    .load(rawSalesDataSource)
}
