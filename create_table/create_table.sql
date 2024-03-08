CREATE EXTERNAL TABLE IF NOT EXISTS prymal_order_log (

order_id STRING
,email STRING
,order_date DATE
, product_rev DOUBLE
, channel STRING


)
PARTITIONED BY 
(
partition_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/order_log/' 
TBLPROPERTIES ("skip.header.line.count"="1")