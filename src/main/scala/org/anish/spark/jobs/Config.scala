package org.anish.spark.jobs

import org.apache.log4j.Logger

/**
  * Created by anish on 17/07/17.
  */

case class Config
(rawSalesDataSource: String = "/data/raw/sales/",
 processedSalesDataHiveTableName: String = "processed.sales",

 rawOmnitureHiveTableName: String = "raw.omniture",
 processedOmnitureHiveTableName: String = "processed.omniture",

 warehouseSessionMappedHiveTableName: String = "warehouse.session_mapped",
 warehousetopSpendersHiveTableName:String = "warehouse.daily_top_spenders",
 warehouseTopSpenderUsageHiveTableName:String = "warehouse.daily_top_spenders_usage_logs",

 topSpendersToAnalyze: Int = 10,

 date_ts: String = "some-hard-coded-default-value",
 runMode: String = "prod"
)

object Config {
  val logger = Logger.getLogger(getClass.getName)

  /**
    * Parse a config object from command line inputs
    * @param args
    * @return
    */
  def parseArgs(args: Array[String]): Config = {
    //    val outputFormats = List("csv", "orc", "parquet")
    //    val sparkSaveModes = List("append", "overwrite", "ErrorIfExists", "Ignore")
    val runModes = List("prod", "data-debug")

    // Generating the parser with all validations
    val parser = new scopt.OptionParser[Config]("spark-submit --class" +
      " <Class-Name> <Jar-Path>") {
      head("ETL on Spark", "v0.1")

      opt[String]("date_ts") required() action {
        (x, c) => c.copy(date_ts = x)
      } text "TimeStamp when the ETL is running"

      opt[String]("rawSalesDataSource") action {
        (x, c) => c.copy(rawSalesDataSource = x)
      } text "Path to Raw Sales Data"

      opt[String]("processedSalesDataHiveTableName") action {
        (x, c) => c.copy(processedSalesDataHiveTableName = x)
      } text "Hive table name for processed Sales Data"

      opt[String]("processedOmnitureHiveTableName") action {
        (x, c) => c.copy(processedOmnitureHiveTableName = x)
      } text "Hive table name for processed Omniture Data"

      opt[Int]("topSpendersToAnalyze") action {
        (x, c) => c.copy(topSpendersToAnalyze = x)
      } text "Number of Top Spenders to Analyze"

      opt[String]("runMode") validate {
        x => if (runModes.contains(x)) success
        else failure(s"Invalid runMode. Currently supported $runModes")
      } action {
        (x, c) => c.copy(runMode = x)
      } text "use 'data-debug' to print intermediate data frames to logs. Defaults to 'prod'"

      help("help") text "prints this usage text"
    }

    parser.parse(args, Config()) match {
      case Some(c) => logger.info("Config for the app is " + c.toString)
        c
      case None => throw new IllegalArgumentException("Invalid config")
    }
  }

}


