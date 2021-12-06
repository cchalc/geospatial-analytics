// Databricks notebook source
// MAGIC %md
// MAGIC # Yellow Trip Data

// COMMAND ----------

// MAGIC %md
// MAGIC The New York City Taxi and Limousine Commission (TLC) provides a [dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) of trips taken by taxis and for-hire vehicles in New York City.  
// MAGIC 
// MAGIC AWS hosts this dataset in an S3 bucket in the `us-east-1` region (more information [here]( https://registry.opendata.aws/nyc-tlc-trip-records-pds/)).
// MAGIC 
// MAGIC Within this dataset, there are records for Yellow taxis, Green taxis, and for-hire vehicles (e.g. Uber, Lyft).  
// MAGIC 
// MAGIC For our purposes, we are interested in the records that have geospatial coordinates associated.  This includes:
// MAGIC - Yellow trip data from January 2009 through June 2016
// MAGIC - Green trip data from August 2013 through June 2016
// MAGIC - (The FHV trip data does not include coordinates)
// MAGIC 
// MAGIC ***IMPORTANT***: Be sure to edit `BASE_PATH_DBFS` before running this notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC # Configuration

// COMMAND ----------

// DBTITLE 1,Configure Paths
val BASE_PATH_DBFS = "/home/derek.yeager@databricks.com/nyctaxi/yellow_tripdata"
// modify ^^this^^ accordingly

val RAW_PATH = BASE_PATH_DBFS + "/raw"
val SCHEMA_V1_RAW_PATH = RAW_PATH + "/schema_v1"
val BRONZE_PATH = "dbfs:" + BASE_PATH_DBFS + "/bronze"
dbutils.fs.mkdirs(SCHEMA_V1_RAW_PATH)

// COMMAND ----------

// MAGIC %md
// MAGIC # ETL - Bronze

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC path=/dbfs/home/derek.yeager@databricks.com/nyctaxi/yellow_tripdata/raw/schema_v1
// MAGIC 
// MAGIC cat <<EOT >> yellow_tripdata_v1.txt
// MAGIC yellow_tripdata_2009-01.csv
// MAGIC yellow_tripdata_2009-02.csv
// MAGIC yellow_tripdata_2009-03.csv
// MAGIC yellow_tripdata_2009-04.csv
// MAGIC yellow_tripdata_2009-05.csv
// MAGIC yellow_tripdata_2009-06.csv
// MAGIC yellow_tripdata_2009-07.csv
// MAGIC yellow_tripdata_2009-08.csv
// MAGIC yellow_tripdata_2009-09.csv
// MAGIC yellow_tripdata_2009-10.csv
// MAGIC yellow_tripdata_2009-11.csv
// MAGIC yellow_tripdata_2009-12.csv
// MAGIC yellow_tripdata_2010-01.csv
// MAGIC yellow_tripdata_2010-02.csv
// MAGIC yellow_tripdata_2010-03.csv
// MAGIC yellow_tripdata_2010-04.csv
// MAGIC yellow_tripdata_2010-05.csv
// MAGIC yellow_tripdata_2010-06.csv
// MAGIC yellow_tripdata_2010-07.csv
// MAGIC yellow_tripdata_2010-08.csv
// MAGIC yellow_tripdata_2010-09.csv
// MAGIC yellow_tripdata_2010-10.csv
// MAGIC yellow_tripdata_2010-11.csv
// MAGIC yellow_tripdata_2010-12.csv
// MAGIC yellow_tripdata_2011-01.csv
// MAGIC yellow_tripdata_2011-02.csv
// MAGIC yellow_tripdata_2011-03.csv
// MAGIC yellow_tripdata_2011-04.csv
// MAGIC yellow_tripdata_2011-05.csv
// MAGIC yellow_tripdata_2011-06.csv
// MAGIC yellow_tripdata_2011-07.csv
// MAGIC yellow_tripdata_2011-08.csv
// MAGIC yellow_tripdata_2011-09.csv
// MAGIC yellow_tripdata_2011-10.csv
// MAGIC yellow_tripdata_2011-11.csv
// MAGIC yellow_tripdata_2011-12.csv
// MAGIC yellow_tripdata_2012-01.csv
// MAGIC yellow_tripdata_2012-02.csv
// MAGIC yellow_tripdata_2012-03.csv
// MAGIC yellow_tripdata_2012-04.csv
// MAGIC yellow_tripdata_2012-05.csv
// MAGIC yellow_tripdata_2012-06.csv
// MAGIC yellow_tripdata_2012-07.csv
// MAGIC yellow_tripdata_2012-08.csv
// MAGIC yellow_tripdata_2012-09.csv
// MAGIC yellow_tripdata_2012-10.csv
// MAGIC yellow_tripdata_2012-11.csv
// MAGIC yellow_tripdata_2012-12.csv
// MAGIC yellow_tripdata_2013-01.csv
// MAGIC yellow_tripdata_2013-02.csv
// MAGIC yellow_tripdata_2013-03.csv
// MAGIC yellow_tripdata_2013-04.csv
// MAGIC yellow_tripdata_2013-05.csv
// MAGIC yellow_tripdata_2013-06.csv
// MAGIC yellow_tripdata_2013-07.csv
// MAGIC yellow_tripdata_2013-08.csv
// MAGIC yellow_tripdata_2013-09.csv
// MAGIC yellow_tripdata_2013-10.csv
// MAGIC yellow_tripdata_2013-11.csv
// MAGIC yellow_tripdata_2013-12.csv
// MAGIC yellow_tripdata_2014-01.csv
// MAGIC yellow_tripdata_2014-02.csv
// MAGIC yellow_tripdata_2014-03.csv
// MAGIC yellow_tripdata_2014-04.csv
// MAGIC yellow_tripdata_2014-05.csv
// MAGIC yellow_tripdata_2014-06.csv
// MAGIC yellow_tripdata_2014-07.csv
// MAGIC yellow_tripdata_2014-08.csv
// MAGIC yellow_tripdata_2014-09.csv
// MAGIC yellow_tripdata_2014-10.csv
// MAGIC yellow_tripdata_2014-11.csv
// MAGIC yellow_tripdata_2014-12.csv
// MAGIC yellow_tripdata_2015-01.csv
// MAGIC yellow_tripdata_2015-02.csv
// MAGIC yellow_tripdata_2015-03.csv
// MAGIC yellow_tripdata_2015-04.csv
// MAGIC yellow_tripdata_2015-05.csv
// MAGIC yellow_tripdata_2015-06.csv
// MAGIC yellow_tripdata_2015-07.csv
// MAGIC yellow_tripdata_2015-08.csv
// MAGIC yellow_tripdata_2015-09.csv
// MAGIC yellow_tripdata_2015-10.csv
// MAGIC yellow_tripdata_2015-11.csv
// MAGIC yellow_tripdata_2015-12.csv
// MAGIC yellow_tripdata_2016-01.csv
// MAGIC yellow_tripdata_2016-02.csv
// MAGIC yellow_tripdata_2016-03.csv
// MAGIC yellow_tripdata_2016-04.csv
// MAGIC yellow_tripdata_2016-05.csv
// MAGIC EOT
// MAGIC 
// MAGIC while read file; do
// MAGIC   wget -P $path http://s3.amazonaws.com/nyc-tlc/trip%20data/$file
// MAGIC done <yellow_tripdata_v1.txt

// COMMAND ----------

// DBTITLE 1,List the raw files
display(dbutils.fs.ls(SCHEMA_V1_RAW_PATH))

// COMMAND ----------

// DBTITLE 1,Define schemas
import org.apache.spark.sql.types._

val schema_v1 = new StructType()
  .add("vendor_name",StringType,true)
  .add("Trip_Pickup_DateTime",TimestampType,true)
  .add("Trip_Dropoff_DateTime",TimestampType,true)
  .add("Passenger_Count",IntegerType,true)  
  .add("Trip_Distance",DoubleType,true)
  .add("Start_Lon",DoubleType,true)
  .add("Start_Lat",DoubleType,true)
  .add("Rate_Code",IntegerType,true)
  .add("store_and_forward",StringType,true) 
  .add("End_Lon",DoubleType,true)
  .add("End_Lat",DoubleType,true)
  .add("Payment_Type",StringType,true)
  .add("Fare_Amt",DoubleType,true)
  .add("surcharge",DoubleType,true)
  .add("mta_tax",DoubleType,true)
  .add("Tip_Amt",DoubleType,true)
  .add("Tolls_Amt",DoubleType,true)
  .add("Total_Amt",DoubleType,true)

val schema_v2 = new StructType()
  .add("vendor_id",StringType,true)
  .add("pickup_datetime",TimestampType,true)
  .add("dropoff_datetime",TimestampType,true)
  .add("passenger_count",IntegerType,true)  
  .add("trip_distance",DoubleType,true)
  .add("pickup_longitude",DoubleType,true)
  .add("pickup_latitude",DoubleType,true)
  .add("rate_code",IntegerType,true)
  .add("store_and_fwd_flag",StringType,true) 
  .add("dropoff_longitude",DoubleType,true)
  .add("dropoff_latitude",DoubleType,true)
  .add("payment_type",StringType,true)
  .add("fare_amount",DoubleType,true)
  .add("surcharge",DoubleType,true)
  .add("mta_tax",DoubleType,true)
  .add("tip_amount",DoubleType,true)
  .add("tolls_amount",DoubleType,true)
  .add("total_amount",DoubleType,true)

val schema_v3 = new StructType()
  .add("VendorID",StringType,true)
  .add("tpep_pickup_datetime",TimestampType,true)
  .add("tpep_dropoff_datetime",TimestampType,true)
  .add("passenger_count",IntegerType,true)  
  .add("trip_distance",DoubleType,true)
  .add("pickup_longitude",DoubleType,true)
  .add("pickup_latitude",DoubleType,true)
  .add("RateCodeID",IntegerType,true)
  .add("store_and_fwd_flag",StringType,true) 
  .add("dropoff_longitude",DoubleType,true)
  .add("dropoff_latitude",DoubleType,true)
  .add("payment_type",StringType,true)
  .add("fare_amount",DoubleType,true)
  .add("extra",DoubleType,true)
  .add("mta_tax",DoubleType,true)
  .add("tip_amount",DoubleType,true)
  .add("tolls_amount",DoubleType,true)
  .add("improvement_surcharge",DoubleType,true) // new column!
  .add("total_amount",DoubleType,true)

// COMMAND ----------

// DBTITLE 1,Read CSV data
val yellow_tripdata_v1 = spark.read
  .format("csv")
  .option("header", "true")
  .schema(schema_v1)
  .load("dbfs:" + SCHEMA_V1_RAW_PATH + "/yellow_tripdata_2009-*.csv")

// COMMAND ----------

// DBTITLE 1,Standardize Column Names
// rename columns to standardize names before reading next batch
val yellow_tripdata_v1_cols_renamed = yellow_tripdata_v1
  .withColumnRenamed("vendor_name", "vendor_id")
  .withColumnRenamed("Trip_Pickup_DateTime", "pickup_datetime")
  .withColumnRenamed("Trip_Dropoff_DateTime", "dropoff_datetime")
  .withColumnRenamed("Passenger_Count", "passenger_count")
  .withColumnRenamed("Trip_Distance", "trip_distance")
  .withColumnRenamed("Start_Lon", "pickup_longitude")
  .withColumnRenamed("Start_Lat", "pickup_latitude")
  .withColumnRenamed("Rate_Code", "rate_code")
  .withColumnRenamed("store_and_forward", "store_and_fwd_flag")
  .withColumnRenamed("End_Lon", "dropoff_longitude")
  .withColumnRenamed("End_Lat", "dropoff_latitude")
  .withColumnRenamed("Payment_Type", "payment_type")
  .withColumnRenamed("Fare_Amt", "fare_amount")
  .withColumnRenamed("Tip_Amt", "tip_amount")
  .withColumnRenamed("Tolls_Amt", "tolls_amount")
  .withColumnRenamed("Total_Amt", "total_amount")

// COMMAND ----------

// DBTITLE 1,load v2 data
val yellow_tripdata_v2 = spark.read
  .format("csv")
  .option("header", "true")
  .schema(schema_v2)
  .load("dbfs:" + SCHEMA_V1_RAW_PATH + "/yellow_tripdata_201[0-4]*.csv")

// COMMAND ----------

// DBTITLE 1,Combine v1 and v2
val union = yellow_tripdata_v1_cols_renamed.union(yellow_tripdata_v2)

// COMMAND ----------

// DBTITLE 1,Write out to Delta
union.write
  .format("delta")
  .mode("overwrite")
  .save(BRONZE_PATH)

// COMMAND ----------

// DBTITLE 1,Create Database
// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS nyctlc;
// MAGIC USE nyctlc;

// COMMAND ----------

// DBTITLE 1,Register table with Metastore
spark.sql("CREATE TABLE yellow_tripdata_bronze USING DELTA LOCATION '" + BRONZE_PATH + "'")

// COMMAND ----------

// DBTITLE 1,Optimize table for performance
// MAGIC %sql
// MAGIC OPTIMIZE nyctlc.yellow_tripdata_bronze

// COMMAND ----------

// MAGIC %md
// MAGIC # Evolve Schema - add new column

// COMMAND ----------

// DBTITLE 1,Load new data
val yellow_tripdata_v3 = spark.read
  .format("csv")
  .option("header", "true")
  .schema(schema_v3)
  .load("dbfs:" + SCHEMA_V1_RAW_PATH + "/yellow_tripdata_201[5-6]*.csv")

// COMMAND ----------

// DBTITLE 1,Standardize column names
val yellow_tripdata_v3_cols_renamed = yellow_tripdata_v3
  .withColumnRenamed("VendorID", "vendor_id")
  .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
  .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
  .withColumnRenamed("RateCodeID", "rate_code")
  .withColumnRenamed("extra", "surcharge")

// COMMAND ----------

// MAGIC %md
// MAGIC # Merge new data

// COMMAND ----------

// DBTITLE 1,Append to the existing Delta table (automatically handles the new column)
yellow_tripdata_v3_cols_renamed.write

  .format("delta")
  .mode("append")

  // automatically add the new column
  .option("mergeSchema", "true")

  .save(BRONZE_PATH)

// COMMAND ----------

// DBTITLE 1,Optimize again
// MAGIC %sql
// MAGIC OPTIMIZE nyctlc.yellow_tripdata_bronze

// COMMAND ----------

// DBTITLE 1,How many records in table?
// MAGIC %sql
// MAGIC SELECT count(*) FROM nyctlc.yellow_tripdata_bronze

// COMMAND ----------

// MAGIC %md 1,213,987,128 records

// COMMAND ----------


