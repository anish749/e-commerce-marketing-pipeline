package org.anish.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by anish on 18/07/17.
  */
object PreparedDataReader {

  def readPreparedOmnitureData(preparedOmnitureDataPath: String)
                              (implicit sqlContext: SQLContext): DataFrame = sqlContext
    .read
    .format("csv")
    .option("header", "true")
    .load(preparedOmnitureDataPath)
    .select("ts", "ip", "url", "userid", "city", "country", "state")
    .withColumn("ts", col("ts").cast(LongType))

  //    .withColumn("ts", unix_timestamp(from_unixtime(col("ts"))))

  def readPreparedSalesData(preparedSalesDataPath: String)
                           (implicit sqlContext: SQLContext): DataFrame = sqlContext
    .read
    .format("csv")
    .option("header", "true")
    .load(preparedSalesDataPath)
    .drop("InvoiceDate")
    .drop("InvoiceNo")


}
