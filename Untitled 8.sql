
use orderdb;

CREATE OR REPLACE STAGE s3_bucket_krish_stage
  URL = 's3://krishregex'
    CREDENTIALS = (AWS_KEY_ID = ''
      AWS_SECRET_KEY = ''
      );

list @s3_bucket_krish_stage;



create or replace table employee(eid int, ename varchar(20), age int);
select * from employee;

-- copy command => load data from the external location to snowflake table
copy into employee
from @s3_bucket_krish_stage
-- files=('products - products (1).csv')
file_format=( field_delimiter='-', skip_header=1);



// Define pipe
CREATE OR REPLACE pipe employee_pipe
auto_ingest = TRUE
AS
copy into employee
from @s3_bucket_krish_stage
file_format=( field_delimiter='-', skip_header=1) ;

// Describe pipe
DESC pipe employee_pipe;
   
SELECT * FROM employee ;