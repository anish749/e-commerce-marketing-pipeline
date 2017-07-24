package org.anish.spark.jobs

import org.anish.spark.RawDataReader
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

/**
  * Created by anish on 20/07/17.
  */
object PrepareSalesData extends SparkJob("Prepare Sales Data") {
  def main(args: Array[String]): Unit = {
    conf = Config.parseArgs(args)
    import hiveContext.implicits._

    val salesData = RawDataReader.readSales(conf.rawSalesDataSource)
      .cache

    val omnitureLogs = RawDataReader.readOmnitureHive(conf.processedOmnitureHiveTableName)
      .cache

    val salesDataIds = salesData
      .select("CustomerID")
      .distinct
      .collect
    val omnitureIds = omnitureLogs
      .select("userid")
      .distinct
      .collect

    val moreSalesIds = salesDataIds ++ salesDataIds ++ salesDataIds
    val zipped = moreSalesIds zip omnitureIds

    val customerIdMapping = zipped.toList.map(m => (m._1.getAs[String](0), m._2.getAs[String](0)))
      .toDF("salesId", "omnitureId")

    // this is for sales data - 541909 records are there
    val gen_ts = (1 to 541909).map(x => (x, (math.random * 10e7).toLong % 86400)).map(x => (x._1, x._2.toLong + 1483228800L))

    val gen_ts_df = gen_ts.toDF("rowNum", "gents")

    val salesDataWithId = salesData.coalesce(1).withColumn("rowNumSales", monotonically_increasing_id() + 1)

    val preparedSalesData = salesDataWithId
      .join(gen_ts_df, 'rowNum === 'rowNumSales)
      .drop('rowNum)
      .drop('rowNumSales)
      .withColumn("generatedInvoiceDate", from_unixtime('gents))
      .drop('gents)

      .join(customerIdMapping, 'salesId === 'CustomerID)
      .withColumnRenamed("omnitureId", "userid")
      .drop('salesId)
      .drop('CustomerID)

    preparedSalesData.write.mode(SaveMode.Overwrite).saveAsTable(conf.processedSalesDataHiveTableName)
  }

}


