create or replace database raw_orders;

create or replace schema stage_schema_ext;

create or replace stage s3_bucket_krish_stage

  URL = 's3://krishregex'
    CREDENTIALS = (AWS_KEY_ID = ''
      AWS_SECRET_KEY = ''
      );
list @raw_orders.stage_schema_ext.s3_bucket_krish_stage;

create or replace schema table_schema_orders;
create or replace schema file_format_schema_orders;

create or replace file format raw_orders.file_format_schema_orders.JSONFORMAT
type = JSON;

-- STRIP_OUTER_ARRAY = TRUE;

create or replace table raw_orders.table_schema_orders.raw_orders1 (
payload variant);

COPY INTO raw_orders.table_schema_orders.raw_orders1
FROM @raw_orders.stage_schema_ext.s3_bucket_krish_stage
FILE_FORMAT = raw_orders.file_format_schema_orders.JSONFORMAT
FILES = ('orders_sample.json');

-- COPY INTO raw_orders.table_schema_orders.raw_orders1
-- FROM @raw_orders.stage_schema_ext.s3_bucket_krish_stage
-- FILE_FORMAT = raw_orders.file_format_schema_orders.JSONFORMAT
-- FILES = ('orders_500.json');

SELECT * FROM raw_orders.table_schema_orders.raw_orders1;

select payload[0]:customer:name::string from raw_orders.table_schema_orders.raw_orders1;

-- select col[1]:customer:name::string from raw_orders.table_schema_orders.raw_orders1;

select f.value:items[0]:attribute ,substr (f.value:customer:customer_id::string , -2) as cid 
from raw_orders.table_schema_orders.raw_orders1,
table(flatten(payload)) f;

-- Q.3

SELECT
    f.value
FROM raw_orders.table_schema_orders.raw_orders1,
     LATERAL FLATTEN(input => payload) f;

SELECT
    f.value:order_id::STRING AS order_id,
    f.value:order_ts::TIMESTAMP_NTZ AS order_ts,
    f.value:customer.customer_id::STRING AS customer_id,
    f.value:customer.name::STRING AS customer_name,
    f.value:customer.address.city::STRING AS city,
    f.value:payment.method::STRING AS payment_method,
    f.value:payment.status::STRING AS payment_status,
    f.value:metadata.source::STRING AS source,
    f.value:metadata.campaign::STRING AS campaign
FROM raw_orders.table_schema_orders.raw_orders1,
     LATERAL FLATTEN(input => payload) f;

-- Q.4

SELECT DISTINCT
    f.value:customer:customer_id::STRING AS customer_id,
    f.value:customer:name::STRING AS customer_name,
    f.value:customer:address:address_line1::STRING AS address_line1,
    f.value:customer:address:city::STRING AS city,
    f.value:customer:address:state::STRING AS state,
    f.value:customer:address:country::STRING AS country,
    f.value:customer:address:pincode::STRING  AS pincode
FROM raw_orders.table_schema_orders.raw_orders1,
     LATERAL FLATTEN(INPUT => payload) f;

-- Q.5

USE DATABASE raw_orders;
USE SCHEMA table_schema_orders;

DESC TABLE raw_orders1;


 DROP TABLE IF EXISTS raw_orders1;

CREATE TABLE raw_orders2 (
    payload VARIANT
);

  COPY INTO raw_orders2
FROM @raw_orders.stage_schema_ext.s3_bucket_krish_stage
FILE_FORMAT = raw_orders.file_format_schema_orders.JSONFORMAT
FILES = ('orders_sample.json');

DESC TABLE raw_orders.table_schema_orders.raw_orders1;

DROP TABLE raw_orders.table_schema_orders.raw_orders1;


CREATE TABLE raw_orders.table_schema_orders.raw_orders1 (
    payload VARIANT
);


COPY INTO raw_orders.table_schema_orders.raw_orders1
FROM @raw_orders.stage_schema_ext.s3_bucket_krish_stage
FILE_FORMAT = raw_orders.file_format_schema_orders.JSONFORMAT
FILES = ('orders_sample.json');

DESC TABLE raw_orders.table_schema_orders.raw_orders1;

CREATE OR REPLACE TABLE raw_orders.table_schema_orders.fact_orders AS
SELECT
    f.value:order_id::STRING             AS order_id,
    f.value:order_ts::TIMESTAMP_NTZ      AS order_ts,
    f.value:customer:customer_id::STRING AS customer_id,
    f.value:payment:method::STRING       AS payment_method,
    f.value:payment:status::STRING       AS payment_status,
    f.value:metadata:source::STRING      AS source,
    f.value:metadata:campaign::STRING    AS campaign
FROM raw_orders.table_schema_orders.raw_orders1,
     LATERAL FLATTEN(INPUT => payload) f;

-- snowpipe

use raw_orders;

list @s3_bucket_krish_stage;

file_format = (field_delimiter = '-' , skip_header = 1);

// Define pipe
USE DATABASE raw_orders;
USE SCHEMA stage_schema_ext;

CREATE OR REPLACE PIPE raw_orders.stage_schema_ext.json_orders_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw_order.table_schema_order.JSON_RAW
FROM @raw_order.stage_schema_ext.s3_stage
FILE_FORMAT = raw_order.file_format_schema_order.JSONFORMAT;

// Describe pipe
desc pipe raw_order.stage_schema_ext.json_orders_pipe;

select * from raw_order.table_schema_order.JSON_RAW;