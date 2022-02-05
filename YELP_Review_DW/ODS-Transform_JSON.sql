USE DATABASE YELPWEATHER;



//Precipitation

CREATE OR REPLACE FILE FORMAT my_csv_format
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = true
    compression = gzip;

SHOW FILE FORMATS;



//Stage local cloud storage

CREATE OR REPLACE STAGE PRECIPITATION file_format=my_csv_format;

// Run on snowsql cli

//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\USW00094789-JFK_INTL_AP-precipitation-inch.csv @PRECIPITATION auto_compress=true;

//list @PRECIPITATION;

SELECT
metadata$filename,
metadata$file_row_number,
parse_json(t.$1), t.$1 from @PRECIPITATION
(file_format => my_csv_format) t;

SELECT to_date($1, 'YYYYMMDD'), $2, $3
FROM @PRECIPITATION (file_format => 'my_csv_format');

CREATE OR REPLACE TABLE NY_PRECIPITATION(
    date date,
    precipitation float,
    precipitation_normal float
);

COPY INTO NY_PRECIPITATION FROM(
    SELECT to_date($1, 'YYYYMMDD'), $2, $3
    FROM @PRECIPITATION (file_format => 'my_csv_format')
);

SELECT *
FROM YELPWEATHER.STAGING.NY_PRECIPITATION
LIMIT 1000;



// TEMPERATURE

//Stage local cloud storage

CREATE OR REPLACE STAGE TEMPERATURE file_format=my_csv_format;

//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\USW00094789-temperature-degreeF.csv @TEMPERATURE auto_compress=true;

//list @TEMPERATURE;

// TEST

SELECT to_date($1, 'YYYYMMDD'), $2, $3, $4, $5
FROM @TEMPERATURE (file_format => 'my_csv_format');

CREATE OR REPLACE TABLE NY_TEMPERATURE(
    date date,
    min_value float,
    max_value float,
    normal_min float,
    normal_max float
);

COPY INTO NY_TEMPERATURE FROM(
    SELECT to_date($1, 'YYYYMMDD'), $2, $3, $4, $5
    FROM @TEMPERATURE (file_format => 'my_csv_format')
);

SELECT *
FROM YELPWEATHER.STAGING.NY_TEMPERATURE
LIMIT 1000;



// COVID

USE SCHEMA STAGING;

SHOW FILE FORMATS;

CREATE OR REPLACE FILE FORMAT myjsonformat type='JSON' strip_outer_array=true;

// Stage local cloud storage
CREATE OR REPLACE STAGE json_stage file_format=myjsonformat;

CREATE TABLE YELP_academic_dataset_covid_features (usersjson variant);

//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\covid_19_dataset\yelp_academic_dataset_covid_features.json @json_stage auto_compress=true;

COPY INTO YELP_COVID
FROM @json_stage/YELP_academic_dataset_covid_features.json.gz
file_format = (format_name = myjsonformat) on_error = 'skip_file';

SELECT *
FROM YELPWEATHER.STAGING.YELP_COVID;

select *
list @json_stage
LIMIT 100;



// Business

CREATE TABLE YELP_Business (usersjson variant);

//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\yelp_dataset\yelp_academic_dataset_business.json @json_stage auto_compress=true;

COPY INTO YELP_Business
FROM @json_stage/yelp_academic_dataset_business.json.gz
file_format = (format_name = myjsonformat) on_error='skip_file';

SELECT *
FROM YELPWEATHER.STAGING.YELP_BUSINESS;



// Checkin

CREATE TABLE YELP_CHECKINS (usersjson variant);
CREATE TABLE YELP_TIPS (usersjson variant);
CREATE TABLE YELP_USERS (usersjson variant);
CREATE TABLE YELP_REVIEWS (usersjson variant);

//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\yelp_dataset\yelp_academic_dataset_checkin.json @json_stage auto_compress=true;
//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\yelp_dataset\yelp_academic_dataset_tip.json @json_stage auto_compress=true;
//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\yelp_dataset\yelp_academic_dataset_user.json @json_stage auto_compress=true;
//put file:///C:\Users\ohike\Desktop\Design a DW for Reporting and OLAP\yelp_dataset\yelp_academic_dataset_review.json @json_stage auto_compress=true;

copy into YELP_CHECKINS
from @json_stage/yelp_academic_dataset_checkin.json.gz
file_format = (format_name = myjsonformat) on_error = 'skip_file';

copy into YELP_TIPS
from @json_stage/yelp_academic_dataset_tip.json.gz
file_format = (format_name = myjsonformat) on_error = 'skip_file';

copy into YELP_USERS
from @json_stage/yelp_academic_dataset_user.json.gz
file_format = (format_name = myjsonformat) on_error = 'skip_file';

copy into YELP_REVIEWS
from @json_stage/yelp_academic_dataset_review.json.gz
file_format = (format_name = myjsonformat) on_error = 'skip_file';

SELECT *
FROM YELPWEATHER.STAGING.YELP_CHECKINS;
SELECT *
FROM YELPWEATHER.STAGING.YELP_TIPS;
SELECT *
FROM YELPWEATHER.STAGING.YELP_USERS;
SELECT *
FROM YELPWEATHER.STAGING.YELP_REVIEWS
LIMIT 100;