-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE events_bronze
COMMENT "RAW Data from Kafka in JSON Format"
AS
SELECT current_timestamp() processing_time, input_file_name() source_file,*
FROM   cloud_files("dbfs:/mnt/dbacademy-datasets/data-engineer-learning-path/v04/ecommerce/raw/events-kafka/","json",map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE events_silver
COMMENT "Silver Events Raw table"
TBLPROPERTIES("quality"="silver")
AS
SELECT timestamp(timestamp) as event_timestamp,* FROM STREAM(LIVE.events_bronze)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE events_by_date
TBLPROPERTIES ("quality"="gold")
AS
SELECT date(event_timestamp) as event_date, 
count(1) as daily_events_total
FROM LIVE.events_silver
group by date(event_timestamp)
