package org.anish.spark.jobs

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by anish on 19/07/17.
  */
abstract class SparkJob(appName: String) {

  val sparkConf = new SparkConf()
    .setAppName(appName)
    .set("spark.sql.shuffle.partitions", "5")

  if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[4]")
  // Else it should run on yarn

  implicit val sparkContext = new SparkContext(sparkConf)

  sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")

  implicit val sqlContext = new SQLContext(sparkContext)
  implicit val hiveContext = new HiveContext(sparkContext)

  hiveContext.setConf("hive.exec.dynamic.partition", "true")
  hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


  implicit var conf:Config = Config() // use default config if not overridden

}
