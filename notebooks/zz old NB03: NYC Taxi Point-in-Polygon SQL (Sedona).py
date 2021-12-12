# Databricks notebook source
# MAGIC %md # Geospatial Processing with Sedona
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/apache/incubator-sedona/master/docs/image/sedona_logo.png" width="200"/>
# MAGIC 
# MAGIC Example point-in-polygon join implementation using Apache Sedona SQL API.
# MAGIC 
# MAGIC Ensure you are using a DBR with Spark 3.0, Java 1.8, Scala 2.12, and Python 3.7+ (tested on 7.3 LTS)
# MAGIC 
# MAGIC Link the following libraries to your cluster:
# MAGIC 
# MAGIC - **Maven Central**
# MAGIC     - `org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating`
# MAGIC     - `org.datasyslab:geotools-wrapper:geotools-24.0`
# MAGIC - **PyPi**
# MAGIC     - `apache-sedona==1.0.1`
# MAGIC     
# MAGIC _More on Sedona libraries and dependencies at https://github.com/apache/incubator-sedona/blob/sedona-1.0.1-incubating/docs/download/maven-coordinates.md_
# MAGIC 
# MAGIC This was run on a 15 node cluster using i3.xlarge AWS instance types.
# MAGIC 
# MAGIC __Authors__
# MAGIC * Initial: [Derek Yeager](https://www.linkedin.com/in/derekcyeager/) (derek@databricks.com)
# MAGIC * Additional: [Michael Johns](https://www.linkedin.com/in/michaeljohns2/) (mjohns@databricks.com)

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

# MAGIC %run ./resources/setup

# COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "1600")

# COMMAND ----------

import os

from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

# COMMAND ----------

spark = SparkSession. \
    builder. \
    appName('Databricks Shell'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:geotools-24.0'). \
    getOrCreate()

# COMMAND ----------

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# MAGIC %md ## Point in Polygon Join Operation on 1% SAMPLE (Non-Optimized)
# MAGIC 
# MAGIC __For smaller data scales, can use Sedona directly with no concern of data + layout. In the example below, __

# COMMAND ----------

# MAGIC %md ### Data

# COMMAND ----------

#what is in the database currently
display(spark.sql(f"show tables from {dbName}")) 

# COMMAND ----------

# MAGIC %md ### Taxi Event Points
# MAGIC 
# MAGIC __Converting Lat/Lon cols to Point Geometry on the fly.__
# MAGIC 
# MAGIC > This is a 1% SAMPLE of the original 45M points (so 450K points)
# MAGIC 
# MAGIC [GeoSparkSQL-Constructor: ST_Point](https://sedona.apache.org/archive/api/sql/GeoSparkSQL-Constructor/?h=st_point#st_point)

# COMMAND ----------

points_df = spark.sql(f"""
  SELECT
    lpep_pickup_datetime,
    Passenger_count,
    Trip_distance,
    Tolls_amount,
    ST_Point(cast(Pickup_longitude as Decimal(24,20)), cast(Pickup_latitude as Decimal(24,20))) as pickup_point 
  FROM 
    {dbName}.green_tripdata_bronze""") \
  .sample(0.01) \
  .repartition(sc.defaultParallelism * 20) \
  .cache()
  
num_points_in_sample = points_df.count()
print(num_points_in_sample)

# COMMAND ----------

# MAGIC %md ### Taxi Zone Polygons
# MAGIC 
# MAGIC __Converting WKT to Polygon Geometries on the fly.__

# COMMAND ----------

polygon_df = spark.sql(f"""
  SELECT 
    zone, 
    ST_GeomFromWKT(the_geom) as geom 
  FROM 
    {dbName}.nyc_taxi_zones_bronze""") \
  .cache()

num_polygons = polygon_df.count()
print(f"num_polygons: {num_polygons}")

# COMMAND ----------

# MAGIC %md ### Summary + Generate Temp Views

# COMMAND ----------

print("points: ", num_points_in_sample)
print("polygons: ", num_polygons)
print("worst case lookups: ", num_points_in_sample * num_polygons)

# COMMAND ----------

points_df.createOrReplaceTempView("points")
polygon_df.createOrReplaceTempView("polygons")

# COMMAND ----------

# MAGIC %md ### Perform `ST_Contains` Point-in-Polygon on 1% SAMPLE (Non-Optimized)
# MAGIC 
# MAGIC __RESULT on 15 worker nodes of AWS i3.xlarge instance type:__ 
# MAGIC 
# MAGIC > This is a 1% SAMPLE of the original 45M points (so 450K points) which takes around 2 minutes to complete

# COMMAND ----------

point_in_polygon = spark.sql("""
  SELECT 
    lpep_pickup_datetime,
    Passenger_count,
    Trip_distance,
    Tolls_amount,
    points.pickup_point, 
    polygons.zone 
  FROM 
    points, polygons 
  WHERE 
    ST_Contains(polygons.geom, points.pickup_point)"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC explain formatted select * from point_in_polygon
# MAGIC -- # point_in_polygon.explain()

# COMMAND ----------

point_in_polygon.show()

# COMMAND ----------

num_matched = point_in_polygon.count()
num_unmatched = num_points_in_sample - num_matched
print("Number of points matched: ", num_matched)
print("Number of points unmatched: ", num_unmatched)

# COMMAND ----------

display(point_in_polygon)

# COMMAND ----------

# MAGIC %md ## Analyze results

# COMMAND ----------

from pyspark.sql.functions import col
display(
  point_in_polygon
    .groupBy("zone")
      .count()
    .sort(col("count").desc())
)

# COMMAND ----------

# MAGIC %md ## Point-In-Polygon + H3 on 100% of DATA (Optimized Storage & Layout in Delta Lake)
# MAGIC 
# MAGIC __RESULT on 15 worker nodes of AWS i3.xlarge instance type:__ 
# MAGIC > !!! This is a 100% of the 45M points (so NO SAMPLING) which ALSO takes around 2 minutes to complete, so nearly 100X faster !!!
# MAGIC 
# MAGIC _Note: This data engineering work was done elsewhere and is added to aid in presentation._

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- what is in the benchmark database currently
# MAGIC show tables from geo_benchmark_nyc

# COMMAND ----------

db_benchmark = "geo_benchmark_nyc"

def_point_cols = ['lpep_pickup_datetime', 'passenger_count', 'trip_distance', 'tolls_amount']
def_polygon_cols = ['zone']

# COMMAND ----------

def display_pip(df_pip, n_points, group_col='zone'):
    """
    Display point-in-polygon results.
    - Will do a group by on col (default is 'zone') and count
    - Also, print counts
    """
    n_matched = df_pip.count()
    
    print(f"Conclusion -->")
    print(f"\tNumber of points matched: {n_matched:,}")
    print(f"\tNumber of points unmatched: {n_points - n_matched:,}")
    
    display(df_pip.groupBy(group_col).count().sort(F.desc("count")))

# COMMAND ----------

def update_spark_confs(d_conf):
    """
    Update spark confs with given dict.
    - Will return back the existing settings for the confs.
    - the settings return can be used to restore the confs.
    """
    used_d = {}
    for k,v in d_conf.items():
        try:
            used_v = spark.conf.get(k)
            print(f"... '{k}' --> existing value {used_v} ({type(used_v)}), new value {v} ({type(v)})")
            spark.conf.set(k, v) 
            used_d[k] = used_v
        except:
            print(f"...unable to set '{k}' to {v}")
            pass
    return used_d

# COMMAND ----------

def gen_point_in_polygon_sql(point_tbl, polygon_tbl, point_col="pickup_latitude_pickup_longitude_jts", polygon_col="the_geom_jts",
                                    point_row_col="gen_row_id", polygon_row_col="gen_row_id", polygon_dirty_col="is_dirty",
                                    point_col_filter= def_point_cols, polygon_col_filter=def_polygon_cols,
                                    point_db=db_benchmark, polygon_db=db_benchmark, 
                                    clear_cache=True, cache_pip=True, 
                                    points_per_part=0, polygons_per_part=0,
                                    point_h3_col=None, polygon_h3_col=None, h3_skew_limit=1000 * 1000,
                                    do_jts_contains=True, use_polygon_dirty=True, return_distinct=False,
                                    print_explain=True, display_result=True, update_conf=None):
    """
    No use of views in this function.
    - See https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-hints.html
      Also, https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-select-hints.html#hints-databricks-sql
    - Optionally cache pip df returned (defaults to True)
      useful for follow on analysis and to get an accurate benchmark time 
      since it calls `.cache().count()`
    - specify point and polygon row id cols (defaults to 'gen_row_id'); expected to have
    - specify how many points to target per partition (defaults to 0); if 0 then not used
    - specify how many polygons to target per partition (defaults to 0); if 0 then not used
    - can specify h3 cols for point and polygon (defaults to None); both must be present to apply
    - h3 skew limit is set to density of > 1,000,000
    - can specify whether to to the jts contains (defaults to true)
    - can use polygon dirty col to further limit need for sedona (defaults to true)
    - can return `distinct` rows (defaults to false)
    - can print explain plan (defaults to true)
    - can display results and print count (defaults to true)
    - can specify spark confs to update and restore (defaults to None)
    """
    # - handle update spark conf
    d_conf = None
    if update_conf is not None:
        d_conf = update_spark_confs(update_conf)
        
    try:
        # - handle clear cache
        if clear_cache:
            sql("""CLEAR CACHE""")
            
        # 1. point cols
        point_col_str = "point.*"
        if point_col_filter is not None:
            point_cols = point_col_filter.copy()
            if point_row_col not in point_cols:
                point_cols.insert(0, point_row_col+" as point_row_id") # add row id
            if point_h3_col is not None:
                point_cols.append(point_h3_col)
            point_col_str = ", ".join([f"point.{c}" for c in point_cols])
        
        # 2. polygon cols
        polygon_col_str = "polygon.*"
        if polygon_col_filter is not None:
            polygon_cols = polygon_col_filter.copy()
            if polygon_row_col not in polygon_cols:
                polygon_cols.insert(0, polygon_row_col+" as polygon_row_id") # add row id
            if polygon_h3_col is not None:
                polygon_cols.append(polygon_h3_col)
            polygon_col_str = ", ".join([f"polygon.{c}" for c in polygon_cols])
        
        # 3. point-in-polygon
        # - calc repartition hint
        polygon_cnt = spark.table(f"{polygon_db}.{polygon_tbl}").count()
        point_cnt = spark.table(f"{point_db}.{point_tbl}").count()
        
        polygon_repartition_str = ""
        if polygons_per_part > 0:
            print("---")
            n_polygon_bins =  math.ceil(polygon_cnt / polygons_per_part)
            print(f"polygon --> cnt: {polygon_cnt}, per part: {polygons_per_part}, n_bins (hint value): {n_polygon_bins}")
            polygon_repartition_str = f"/*+ REPARTITION({n_polygon_bins}) */"
        
        point_repartition_str = ""
        if points_per_part > 0:
            if len(polygon_repartition_str) == 0:
                print("---")
            n_point_bins =  math.ceil(point_cnt / points_per_part)
            print(f"point --> cnt: {point_cnt}, per part: {points_per_part}, n_bins (hint value): {n_point_bins}")
            point_repartition_str = f"/*+ REPARTITION({n_point_bins}) */"
            
        # - handle H3
        skew_hint = ""
        h3_restriction = ""
        if polygon_h3_col is not None and point_h3_col is not None:
            point_skews = [f"'{r[0]}'" for r in spark.table(f"{point_db}.{point_tbl}").groupBy(point_h3_col).count().filter(f"count > {h3_skew_limit}").collect()]
            skew_hint = f"""/*+ SKEW( 'point', '{point_h3_col}', ({", ".join(point_skews)}) ) */"""
            # - h3 restriction
            # must be in the same index space
            h3_restriction = f"point.{point_h3_col} = polygon.{polygon_h3_col}"
            
        # - handle jts contains
        # also, handle optional polygon dirty
        jts_contains = ""
        if do_jts_contains and not use_polygon_dirty:
            jts_contains = f"ST_Contains(polygon.{polygon_col}, point.{point_col})"
        elif do_jts_contains and use_polygon_dirty:
            jts_contains = f"( polygon.{polygon_dirty_col} = FALSE OR ST_Contains(polygon.{polygon_col}, point.{point_col}) )"
        
        # - handle combo
        where_and = ""
        if len(h3_restriction) > 0 and len(jts_contains) > 0:
            where_and = " AND "
        
        # - sql
        pip_sql = f"""
        SELECT {skew_hint} 
          {polygon_col_str}, 
          {point_col_str} 
        FROM 
          ( SELECT {polygon_repartition_str} * FROM {polygon_db}.{polygon_tbl} ) as polygon, 
          ( SELECT {point_repartition_str} * FROM {point_db}.{point_tbl} ) as point 
        WHERE
          {h3_restriction}{where_and}{jts_contains}
        """
        
        # - handle explain
        if print_explain:
            print("--- explain -->")
            print(spark.sql(pip_sql).explain())
        
        print("---")
        print("pip_sql -->", pip_sql)
        df_pip = spark.sql(pip_sql)
        
        # - handle distinct
        if return_distinct:
            df_pip = df_pip.distinct()
        
        # - handle cache
        if cache_pip:
            df_pip.cache().count()
        
        # - handle display
        if display_result:
            display_pip(df_pip, point_cnt)
            
    finally:
        print("restoring spark conf settings...") 
        if d_conf is not None:
            update_spark_confs(d_conf)
        
    return df_pip

# COMMAND ----------

df_pip8 = gen_point_in_polygon_sql(
    "green_tripdata_geo_idx_h3_8_pickup_latitude_pickup_longitude", "nyc_taxi_zones_geo_idx_h3_8_the_geom", cache_pip=True, 
    points_per_part=0, polygons_per_part=0, 
    point_h3_col="h3_8_pickup_latitude_pickup_longitude", polygon_h3_col="h3_8_the_geom", display_result=True,
    h3_skew_limit=100 * 1000, do_jts_contains=True, use_polygon_dirty=True, update_conf={'spark.sql.shuffle.partitions': 1600} # <-- toggle these for benchmarks 
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Notice the injection of testing H3 index space as well as the use of `polygon.is_dirty` expression in the `WHERE` clause.
# MAGIC 
# MAGIC ```
# MAGIC WHERE point.h3_8_pickup_latitude_pickup_longitude = polygon.h3_8_the_geom AND ( polygon.is_dirty = FALSE OR ST_Contains(polygon.the_geom_jts, point.pickup_latitude_pickup_longitude_jts) )
# MAGIC ```

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC -- THIS IS THE SQL GENERATED which can be run directly (since already cached will run near instantly)
# MAGIC 
# MAGIC SELECT /*+ SKEW( 'point', 'h3_8_pickup_latitude_pickup_longitude', ('882a100de3fffff', '882a100dc7fffff', '882a100dc5fffff', '882a100de5fffff', '882a100dbdfffff', '882a100a87fffff', '882a100aa1fffff', '882a100a81fffff', '882a100dedfffff', '882a100f37fffff', '882a100f3dfffff', '882a100d43fffff', '882a100d3bfffff', '882a100da5fffff', '882a100da7fffff', '882a100da1fffff', '882a100da3fffff', '882a1008c3fffff', '882a1008c1fffff', '882a107453fffff', '882a1072d7fffff', '882a100d87fffff', '882a100da9fffff', '882a100dabfffff', '882a100dadfffff', '882a107291fffff', '882a107293fffff', '882a1008d9fffff', '882a1008d7fffff', '882a100d17fffff', '882a100d33fffff', '882a100899fffff', '882a100ea9fffff', '882a100e83fffff', '882a100f13fffff', '882a100f07fffff', '882a100d5bfffff', '882a100d85fffff', '882a100d83fffff', '882a1008ddfffff', '882a1008dbfffff', '882a100c47fffff', '882a100c4dfffff', '882a100c45fffff', '882a100c49fffff', '882a100aa9fffff', '882a100ab1fffff', '882a100ab5fffff', '882a100c43fffff', '882a100c09fffff', '882a107749fffff', '882a107741fffff', '882a100a17fffff', '882a1008cbfffff', '882a1008c9fffff', '882a1008d1fffff', '882a100f33fffff', '882a100f35fffff', '882a100e07fffff', '882a100c69fffff', '882a100c6bfffff', '882a100d09fffff', '882a100d01fffff', '882a100d03fffff', '882a107299fffff', '882a10729bfffff', '882a100f31fffff', '882a10774dfffff', '882a100db1fffff', '882a100db5fffff', '882a100db9fffff', '882a100889fffff', '882a100c65fffff', '882a1008d3fffff', '882a100d15fffff', '882a100d0dfffff', '882a100c5bfffff', '882a100c55fffff', '882a1008d5fffff', '882a100ac9fffff', '882a100abbfffff', '882a100ae1fffff', '882a100de9fffff', '882a100aa3fffff', '882a100ae7fffff', '882a100c61fffff', '882a1072d3fffff', '882a1072d1fffff', '882a100ab9fffff', '882a100ab7fffff', '882a100d55fffff', '882a100d51fffff', '882a100d4bfffff') ) */ 
# MAGIC           polygon.gen_row_id as polygon_row_id, polygon.zone, polygon.h3_8_the_geom, 
# MAGIC           point.gen_row_id as point_row_id, point.lpep_pickup_datetime, point.passenger_count, point.trip_distance, point.tolls_amount, point.h3_8_pickup_latitude_pickup_longitude 
# MAGIC         FROM 
# MAGIC           ( SELECT  * FROM geo_benchmark_nyc.nyc_taxi_zones_geo_idx_h3_8_the_geom ) as polygon, 
# MAGIC           ( SELECT  * FROM geo_benchmark_nyc.green_tripdata_geo_idx_h3_8_pickup_latitude_pickup_longitude ) as point 
# MAGIC         WHERE
# MAGIC           point.h3_8_pickup_latitude_pickup_longitude = polygon.h3_8_the_geom AND ( polygon.is_dirty = FALSE OR ST_Contains(polygon.the_geom_jts, point.pickup_latitude_pickup_longitude_jts) )
