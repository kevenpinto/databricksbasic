-- Databricks notebook source
create or refresh streaming live table orders_bronze
as
select current_timestamp() processing_time,
      input_file_name() source_file,
      *
from cloud_files("${source_path}/orders/stream_json"
,"json"
,map("cloudFiles.inferColumnTypes","true"))  

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE orders_silver
COMMENT "Orders Silver Table"
TBLPROPERTIES ("quality"="silver")
AS
SELECT  timestamp(order_timestamp) as order_timestamp,
        * EXCEPT(order_timestamp, source_file, _rescued_data)
FROM    STREAM(LIVE.orders_bronze)

-- COMMAND ----------

create or refresh live table orders_by_date
as
select date(order_timestamp) as order_date,
      count(1) as total_daily_orders
FROM live.orders_silver
group by date(order_timestamp)
