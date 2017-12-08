#!/usr/bin/env bash

[ $# -ne 2 ] && { echo "Usage: $0 local_omniture_source_dir local_sales_source_dir" ; exit 1; }

hadoop_success_file=/user/ec2-user/data/raw/_SUCCESS

# Check if a success file is present, which means the previously loaded data has not been ingested yet.
if $(hdfs dfs -test -f $hadoop_success_file) ; then { echo "Earlier data not ingested yet"; exit 1; }; fi


ts=$(date "+%Y%m%d_%H") # Precise to hour. Add %M%S to make it more precise

echo "Time Stamp used is $ts"

local_omniture_source_dir=$1
hadoop_omniture_data_dir=/user/ec2-user/data/raw/omniture/$ts

hdfs dfs -mkdir -p $hadoop_omniture_data_dir && \
hdfs dfs -put $local_omniture_source_dir $hadoop_omniture_data_dir && \
hdfs dfs -touchz $hadoop_omniture_data_dir/_SUCCESS && \
echo "Successfully pushed Omniture data from $local_omniture_source_dir to $hadoop_omniture_data_dir"
omnitureLoad=$?

local_sales_source_dir=$2
hadoop_sales_data_dir=/user/ec2-user/data/raw/sales/$ts

hdfs dfs -mkdir -p $hadoop_sales_data_dir && \
hdfs dfs -put $local_sales_source_dir $hadoop_sales_data_dir && \
hdfs dfs -touchz $hadoop_sales_data_dir/_SUCCESS && \
echo "Successfully pushed Sales data from $local_sales_source_dir to $hadoop_sales_data_dir"
salesLoad=$?

if [ $omnitureLoad -eq 0 -a $salesLoad -eq 0 ]
then
    hdfs dfs -touchz $hadoop_success_file && \
    echo "Successfully created _SUCCESS file at $hadoop_success_file"
fi
