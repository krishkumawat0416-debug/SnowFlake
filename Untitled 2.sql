create or replace database jsondb;

create or replace schema stage_schema_ext ;

// First step : Load Raw Json

create or replace STAGE s3_bucket_krish_stage

    URL = 's3://krishregex'
    CREDENTIALS = (AWS_KEY_ID = ''
      AWS_SECRET_KEY = ''

 list  @jsondb.stage_schema_ext.s3_bucket_krish_stage;

 -- schema for tablw

 create or replace schema table_schema_hr;
 create or replace schema file_format_Schema_hr;


 CREATE OR REPLACE file format jsondb.file_format_schema_hr.JSONFORMAT
    TYPE = JSON;

 CREATE OR REPLACE TABLE jsondb.table_schema_hr.JSON_RAW (   
    raw_file variant );

 select * from jsondb.table_schema_hr.JSON_RAW; 
 copy into jsondb.stage_schema_ext.s3_bucket_krish_stage
 file_format=jsondb.file_format_schema_hr.JSONFORMAT
 files = ('HR_Data(1).json');