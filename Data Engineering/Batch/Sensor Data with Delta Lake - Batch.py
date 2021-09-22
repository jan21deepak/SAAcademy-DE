# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ACID Transactions with Delta Lake (IoT Data)
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC This is a companion notebook to provide a Delta Lake example against the IoT data.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

setup_responses = dbutils.notebook.run("../Utils/Setup-Batch-GDrive", 0).split()

dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]
bronze_table_path = f"{dbfs_data_path}tables/bronze"
silver_table_path = f"{dbfs_data_path}tables/silver"
silver_clone_table_path = f"{dbfs_data_path}tables/silver_clone"
silver_sh_clone_table_path = f"{dbfs_data_path}tables/silver_clone_shallow"
silver_constraints_table_path = f"{dbfs_data_path}tables/silver_constraints"
gold_table_path = f"{dbfs_data_path}tables/gold"
gold_agg_table_path = f"{dbfs_data_path}tables/goldagg"
parquet_table_path = f"{dbfs_data_path}tables/parquet"
autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"
dbutils.fs.rm(bronze_table_path, recurse=True)
dbutils.fs.rm(silver_table_path, recurse=True)
dbutils.fs.rm(gold_table_path, recurse=True)
dbutils.fs.rm(parquet_table_path, recurse=True)
dbutils.fs.rm(silver_clone_table_path, recurse=True)
dbutils.fs.rm(gold_agg_table_path, recurse=True)

print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))
print("Brone Table Location is {}".format(bronze_table_path))
print("Silver Table Location is {}".format(silver_table_path))
print("Gold Table Location is {}".format(gold_table_path))
print("Parquet Table Location is {}".format(parquet_table_path))

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## About the Data
# MAGIC 
# MAGIC The data used is mock data for 2 types of devices - Transformer/ Rectifier from 3 power plants generating 3 set of readings relevant to monitoring the status of that device type
# MAGIC 
# MAGIC ![ioT_Data](https://miro.medium.com/max/900/1*M_Q4XQ4pTCuANLyEZqrDOg.jpeg)

# COMMAND ----------

dataPath = f"{dbfs_data_path}historical_sensor_data.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("bronze_readings_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from bronze_readings_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ETL Flow - Batch

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Architecture
# MAGIC 
# MAGIC <img src="https://delta.io/wp-content/uploads/2019/04/Delta-Lake-marketecture-0423c.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reliability
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_reliability.png?raw=true" width=1000/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_performance.png?raw=true" width=1000/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Delta Lake Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/simplysaydelta.png" width=800/>

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS sensor_readings_historical_bronze USING DELTA LOCATION '{bronze_table_path}' AS SELECT * from bronze_readings_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What does Delta Log look like?

# COMMAND ----------

df = spark.sql("DESCRIBE HISTORY sensor_readings_historical_bronze")

# COMMAND ----------

dbutils.fs.ls(bronze_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date range & data volume for current Bronze Data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Maybe Date would be a good way to partition the data
# MAGIC 
# MAGIC SELECT DISTINCT DATE(reading_time), count(*) FROM sensor_readings_historical_bronze group by DATE(reading_time)
# MAGIC 
# MAGIC -- Hmmm, there are only two dates, so maybe that's not the best choice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Step 1
# MAGIC ### Create Silver Table 
# MAGIC #### There is some missing data. Time to create a silver table, backfill and transform!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver;

# COMMAND ----------

spark.sql(f"CREATE TABLE if not exists sensor_readings_historical_silver USING DELTA LOCATION '{silver_table_path}' AS SELECT * from bronze_readings_view")

# COMMAND ----------

dataPath = f"{dbfs_data_path}backfill_sensor_data_final.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("backfill_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from backfill_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT DATE(reading_time) as BF_DATE, count(*) as BF_COUNT FROM backfill_view group by DATE(reading_time);

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE with Parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake: 2-step process
# MAGIC 
# MAGIC With Delta Lake, inserting or updating a table is a simple 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use the `MERGE` command

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO sensor_readings_historical_silver AS SL
# MAGIC USING backfill_view AS BF
# MAGIC ON 
# MAGIC   SL.id = BF.id
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Validate the output of merge

# COMMAND ----------

# MAGIC %md
# MAGIC You should see data for a 21st of Feb + an increased count for 23rd of Feb

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT DATE(reading_time) as SL_DATE, count(*) as SL_COUNT FROM sensor_readings_historical_silver group by DATE(reading_time);

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## ETL Step 2
# MAGIC 
# MAGIC ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support
# MAGIC 
# MAGIC Let us delete data for device_operational_status = 'CORRUPTED'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_operational_status, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_operational_status
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `DELETE` on the Delta table
# MAGIC DELETE FROM sensor_readings_historical_silver WHERE device_operational_status = 'CORRUPTED'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_operational_status, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_operational_status
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ETL Step 3
# MAGIC ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) UPDATE Support
# MAGIC There is one record which has a wrong device id '7G007TTTTT' instead of '7G007T'. Let us update this

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_id
# MAGIC ORDER BY count ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Attempting to run `UPDATE` on the Delta table
# MAGIC UPDATE sensor_readings_historical_silver SET `device_id` = '7G007T' WHERE device_id = '7G007TTTTT'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(*) AS count FROM sensor_readings_historical_silver
# MAGIC GROUP BY device_id
# MAGIC ORDER BY count ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance Benefits

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading CSV

# COMMAND ----------

dataPath = f"{dbfs_data_path}historical_sensor_data.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading Delta

# COMMAND ----------

df = spark.read.option("format", "delta").load(bronze_table_path)
df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_bronze WHERE device_id = '7G007R' AND device_operational_status != 'NOMINAL'

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sensor_readings_historical_bronze ZORDER BY (device_id, device_operational_status)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_bronze WHERE device_id = '7G007R' AND device_operational_status != 'NOMINAL'

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Create Gold table
# MAGIC Now that our Silver table has been cleaned and conformed, and we've evolved the schema, the next step is to create a Gold table. Gold tables are often created to provide clean, reliable data for a specific business unit or use case.
# MAGIC 
# MAGIC In our case, we'll create a Gold table that joins the silver table with the Plant dimension - to provide an aggregated view of our data. For our purposes, this table will allow us to show what Delta Lake can do, but in practice a table like this could be used to feed a downstream reporting or BI tool that needs data formatted in a very specific way. Silver tables often feed multiple downstream Gold tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_plant

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sensor_readings_historical_gold_view
# MAGIC AS
# MAGIC SELECT a.plant_id, a.device_id, a.plant_type, b.device_type, b.device_operational_status, b.reading_time, b.reading_1, b.reading_2, b.reading_3
# MAGIC FROM dim_plant a INNER JOIN sensor_readings_historical_silver b
# MAGIC ON a.device_id = b.device_id

# COMMAND ----------

spark.sql(f"CREATE TABLE if not exists sensor_readings_gold USING DELTA LOCATION '{gold_table_path}' AS SELECT * FROM sensor_readings_historical_gold_view")

# COMMAND ----------

spark.sql(f"CREATE TABLE if not exists sensor_readings_gold_agg USING DELTA LOCATION '{gold_agg_table_path}' AS Select plant_id, plant_type, device_type, device_operational_status, count(*) as count from sensor_readings_historical_gold_view group by plant_id, plant_type, device_type, device_operational_status")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from sensor_readings_gold_agg
