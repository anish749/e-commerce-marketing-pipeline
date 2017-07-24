#!/usr/bin/env bash

hdfsCodeDir=/code

hdfs dfs -rm -r $hdfsCodeDir
hdfs dfs -mkdir -p $hdfsCodeDir && \
hdfs dfs -put `dirname $0`/hdfs/* $hdfsCodeDir && \
echo "Deployed codes to $hdfsCodeDir"

sudo ln -fs /usr/lib/hive/conf/hive-site.xml /usr/lib/spark/conf/hive-site.xml && \
echo "Linked hive-site.xml to spark conf"

hdfs dfs -put /usr/lib/hive/conf/hive-site.xml /code/hive/hive-site.xml && \
echo "Copied hive-site.xml to HDFS for Oozie"

