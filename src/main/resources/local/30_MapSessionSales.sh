#!/usr/bin/env bash

date_ts=${1-20170724_11}
echo "Setting date_ts to $date_ts ..."

spark-submit \
--master yarn-client \
--verbose \
--driver-memory 2g --executor-memory 4g \
--packages com.databricks:spark-csv_2.10:1.5.0 \
--conf spark.sql.parquet.compression.codec=snappy \
--conf spark.sql.parquet.filterPushdown=true \
--conf spark.sql.parquet.mergeSchema=false \
--conf spark.sql.parquet.filterPushdown=true \
--conf spark.sql.hive.metastorePartitionPruning=true \
--class org.anish.spark.jobs.MapSessionSales \
`dirname $0`/../hdfs/spark/e-commerce-marketing-spark.jar \
--rawSalesDataSource /data/raw/sales/$date_ts/ \
--date_ts $date_ts
