-- Databricks notebook source
CREATE or REFRESH STREAMING LIVE TABLE customers_bronze
COMMENT "Raw Data from Customers CDC Feed"
AS
SELECT current_timestamp() processing_time,
        input_file_name() source_file,
        *
FROM cloud_files("${source_path}/customers/stream_json","json")


-- COMMAND ----------

CREATE STREAMING TABLE customers_bronze_clean
(CONSTRAINT valid_id EXPECT(customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT(operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT(name IS NOT NULL or operation = "DELETE"),
CONSTRAINT valid_address EXPECT((address IS NOT NULL 
and city IS NOT NULL and state IS NOT NULL and zip_code IS NOT NULL) or operation ="DELETE"),
CONSTRAINT valid_email EXPECT (
  rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
  operation = "DELETE") ON VIOLATION DROP ROW
) AS
SELECT *
FROM STREAM(LIVE.customers_bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
FROM STREAM(LIVE.customers_bronze_clean)  
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY timestamp
COLUMNS * EXCEPT(operation, source_file, _rescued_data)


-- COMMAND ----------



-- COMMAND ----------

CREATE LIVE VIEW subscribed_order_emails_v
  AS SELECT a.customer_id, a.order_id, b.email 
    FROM LIVE.orders_silver a
    INNER JOIN LIVE.customers_silver b
    ON a.customer_id = b.customer_id
    WHERE notifications = 'Y'
