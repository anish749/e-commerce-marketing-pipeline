package org.anish.spark.session

import org.anish.spark.jobs.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by anish on 18/07/17.
  */
object SessionSalesMapper {

  def mapSalesToSessions(omnitureData: DataFrame, salesData: DataFrame)
                        (implicit hiveContext: HiveContext, config: Config): DataFrame = {
    import hiveContext.implicits._

    val sessionDf = omnitureData
      .groupBy('userid, 'sessionid)
      .agg(min('ts) as "sessionStartTime", max('ts) as "sessionEndTime")

    //    join sessionDf with salesData (approx join)
    sessionDf.join(salesData, sessionDf("userid") === salesData("userid") &&
      unix_timestamp('generatedInvoiceDate) < 'sessionEndTime &&
      unix_timestamp('generatedInvoiceDate) > 'sessionStartTime
      , "left"
    )
      .drop(salesData("userid"))
      .withColumn("date_ts", lit(config.date_ts))
  }

}
