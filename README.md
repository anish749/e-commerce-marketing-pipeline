E commerce marketing Pipeline
___

An ETL module meant to be used for ingesting and storing data to data lakes which can be queried for analyzing data from time to time
This also has a feature to find users to send promotional mailers.

This is made as an example use case only using data available in the public domain to showcase how work flows and data pipelines work in the Hadoop ecosystem with Oozie, Hive and Spark.


### Data Sources used:
 - Web logs recorded by Omniture (Adobe Site Catalyst) 
 - Point of Sale data for e-commerce (retail sales)
 
### ETL logic
 - Take data from web logs and create sessions. A session is defined as period of time during which there was continuous clicks from the user. The max idle time in between clicks in one session is considered 30 mins. Different ip addresses result in different sessions (we use IP address here, but ideally it should be the cookie id.)
 - Search for sales that happened in a given session by trying to correlate sales data for a particular user in that session duration.
 - Search for top spenders in a given hour / day and store their user ids separately for sending promotional messages (marketing)
 - Separate out usage logs for top spenders in a warehouse table.

### Platform use cases
 - Analysts can access and run BI queries on consumer usage data / usage data of only top spenders.
 - Analyze page visit patterns which led to sales, and ones which didn't.
 
## Demo:

### Compiling and running:
##### Versions used and tested:
 - Scala 2.10.4
 - Spark 1.6.0
 - Java 1.8 and Maven 3.3.9
 - Cloudera Quick Start VM 5.10
 
##### Download data from:
 - [Online Retail data](http://archive.ics.uci.edu/ml/datasets/Online+Retail) Then convert to csv from excel.
 - [Omniture](https://s3.amazonaws.com/hw-sandbox/tutorial8/RefineDemoData.zip) This has quite a bit of logs, product info, users, etc, from Hortonworks
 
(Re)-compile as follows
```
rm -rf build-files && \
mvn clean package
```

All jars, hive queries, oozie workflows and shell scripts required to run in VM are created in the build-files folder.
Deploy these to VM. Alternatively create ~/codeDrops directory in VM and you can run deployToVM.sh from host machine, which would package and deploy to VM.

Once logged in to VM, run ```setupHdfs.sh``` to deploy suitable codes to HDFS, and place hive-site.xml in proper places.
```NOTE:``` Oozie uses hive-site.xml as job-xml while running hive jobs. Here we place the hive-site.xml available to HDFS and link it to Oozie. In case hive-site is changed, you need to manually place this file again in HDFS.

Run ```local/00_SetupHive.sh``` to trigger a Oozie workflow which creates Hive database and tables required for setup.

Set start time and end time properties in ```oozie_coordinator.properties``` and submit the coordinator using ```10_SubmitCoordinator.sh```
The coordinator looks for files to arrive in HDFS and triggers the prepare-input-data workflow.

Ingest some sample data using ```15_IngestData.sh```

Ideally this should be end of manual work, and oozie should trigger the actions one after another to ingest the data.

With less memory, spark submit would have to be done manually.
These steps are:
 - Prepare sales data - Spark job - Prepare the sales data to be usable with Omniture data.
 - Map session sales - Spark job - Map the sales data with the Omniture data.
 - Daily Top Spenders - Spark job - Find top spenders and separate out their usage.

#### Hive table schema
 - Database raw = As and when new data arrives, it is added as a partition to tables in this db
 - Database processed = This is a temporary db where prepared data is stored.
 - Database warehouse = This is the final warehouse where data is partitioned by time (hr) and stored for analysts to query.
 
### Querying processed data

Log into the Spark Shell and use the following:
```scala
import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
hiveContext.table("warehouse.session_mapped").show
hiveContext.table("warehouse.daily_top_spenders").show
hiveContext.table("warehouse.daily_top_spenders_usage_logs").show
```


___
