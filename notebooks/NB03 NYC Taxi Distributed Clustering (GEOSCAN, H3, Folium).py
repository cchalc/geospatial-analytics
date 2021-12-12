# Databricks notebook source
# MAGIC %md # Distributed Clustering with GEOSCAN
# MAGIC 
# MAGIC <img src="https://1fykyq3mdn5r21tpna3wkdyi-wpengine.netdna-ssl.com/wp-content/uploads/2018/06/image12.png" alt="drawing" width="200"/>
# MAGIC 
# MAGIC Demonstrates [GEOSCAN](https://github.com/databrickslabs/geoscan) on the [nyctaxi](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) dataset (green trips only).  See Antoine Amend's blog article "[Identifying Financial Fraud With Geospatial Clustering](https://databricks.com/blog/2021/04/13/identifying-financial-fraud-with-geospatial-clustering.html)" for more info on GEOSCAN.
# MAGIC 
# MAGIC __Libraries__
# MAGIC * Add the following maven coordinates to your cluster: `com.databricks.labs:geoscan:0.1` and `com.uber:h3:3.6.3`
# MAGIC * The first code cell will pip install a session scoped libraries for geoscan, h3, and folium
# MAGIC 
# MAGIC _This was run on DBR ML 7.3 with 3 worker nodes of AWS instance type i3.xlarge. The machine learning runtime is needed for the mlflow integration demonstrated._
# MAGIC 
# MAGIC __Authors__
# MAGIC * Initial: [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC * Additional: [Michael Johns](https://www.linkedin.com/in/michaeljohns2/) (mjohns@databricks.com)
# MAGIC 
# MAGIC __GEOINT 2021 Slides for Training Class [Databricks: Hands-On Scaled Spatial Processing using Popular Open Source Frameworks](https://docs.google.com/presentation/d/1oqEh-FD5Ls5xgo-OOEUbuzA2tMzNFQ8V/edit?usp=sharing&ouid=110699193143161784974&rtpof=true&sd=true) by Michael Johns__

# COMMAND ----------

# MAGIC %md ## Initial Config

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databrickslabs/geoscan.git#subdirectory=python h3==3.6.3 folium

# COMMAND ----------

# MAGIC %sql USE nyctlc

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

points_df = spark.read.table('green_tripdata_bronze') \
  .withColumnRenamed("Pickup_longitude", "longitude") \
  .withColumnRenamed("Pickup_latitude", "latitude") \
  .drop("VendorID") \
  .drop("lpep_pickup_datetime") \
  .drop("Lpep_dropoff_datetime") \
  .drop("Store_and_fwd_flag") \
  .drop("RateCodeID") \
  .drop("Dropoff_longitude") \
  .drop("Dropoff_latitude") \
  .drop("Passenger_count") \
  .drop("Trip_distance") \
  .drop("Fare_amount") \
  .drop("Extra") \
  .drop("MTA_tax") \
  .drop("Tip_amount") \
  .drop("Tolls_amount") \
  .drop("Ehail_fee") \
  .drop("Total_amount") \
  .drop("Payment_type") \
  .drop("Trip_type") \
  .drop("improvement_surcharge")

num_points = points_df.count()
print("num_points: ", num_points)

#display(points_df)
