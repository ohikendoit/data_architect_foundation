USE DATABASE YELPWEATHER;

CREATE SCHEMA DWH;

USE SCHEMA DWH;



//Precipitation
CREATE OR REPLACE TABLE DIM_PRECIPITATION(
    date date,
    precipitation float,
    precipitation_normal float
);

//Load
INSERT INTO DWH.DIM_PRECIPITATION SELECT * FROM ODS.PRECIPITATION;

//Sample data
SELECT *
FROM DWH.DIM_PRECIPITATION
LIMIT 1000;



//Temperature
SELECT *
FROM ODS.TEMPERATURE
LIMIT 1000;

CREATE OR REPLACE TABLE DIM_TEMPERATURE(
    date date,
    min float,
    max float,
    normal_min float,
    normal_max float
);

//Load
INSERT INTO DWH.DIM_TEMPERATURE SELECT * FROM ODS.TEMPERATURE;

//Sample data
SELECT *
FROM DWH.DIM_TEMPERATURE
LIMIT 1000;



// Business
SELECT *
FROM ODS.YELP_BUSINESS
LIMIT 100;

CREATE OR REPLACE TABLE DIM_BUSINESS(
    business_id string,
    name string,
    address string,
    city string,
    state string,
    postal_code string,
    latitude float,
    longitude float,
    stars float,
    review_count number,
    is_open number,
    attributes variant,
    categories string,
    hours variant,
    constraint pk_business_id primary key (business_id)
);

//Load
INSERT INTO DIM_BUSINESS

//Sample data
SELECT *
FROM ODS.YELP_BUSINESS
LIMIT 1000;



// Users
SELECT *
FROM ODS.YELP_USER
LIMIT 100;

CREATE OR REPLACE TABLE DIM_USER(
    user_id string,
    name string,
    review_count string,
    yelping_since timestamp,
    useful number,
    funny number,
    cool number,
    elite string,
    friends variant,
    fans number,
    average_stars float,
    compliment_hot number,
    compliment_more number,
    compliment_profile number,
    compliment_cute number,
    compliment_list number,
    compliment_note number,
    compliment_plain number,
    compliment_cool number,
    compliment_funny number,
    compliment_writer number,
    compliment_photos number,
    constraint pk_user_id primary key (user_id)
);

INSERT INTO DIM_USER
SELECT *
FROM ODS.YELP_USER;

SELECT *
FROM DWH.DIM_USER
LIMIT 100;

// Reviews

SELECT *
FROM ODS.YELP_REVIEW
LIMIT 100;

CREATE OR REPLACE TABLE DIM_REVIEW(
    review_id string,
    user_id string,
    business_id string,
    stars float,
    useful number,
    funny number,
    cool number,
    text string,
    date timestamp_ntz,
    constraint pk_review_id primary key(review_id)
 );

 INSERT INTO DIM_REVIEW
 SELECT *
 FROM ODS.YELP_REVIEW;

 SELECT *
 FROM DWH.DIM_REVIEW
 LIMIT 1000;



// Fact Table

CREATE OR REPLACE TABLE FACT_CLIMATE_REVIEW(
    date date,
    review_id string,
    business_id string,
    user_id string,
    min_temperature float,
    max_temperature float,
    precipitation float,
    stars float,
    primary key (date, review_id)
    foreign key (business_id) REFERENCES DIM_REVIEW(business_id)
    foreign key (user_id) REFERENCES DIM_REVIEW(user_id)
);

INSERT INTO FACT_CLIMATE_REVIEW
SELECT r.DATE, r.REVIEW_ID, r.BUSINESS_ID, r.USER_ID, t.MIN, t.MAX, p.PRECIPITATION, r.STARS
FROM DWH.DIM_REVIEW r
JOIN DWH.DIM_USER u ON u.USER_ID = r.USER_ID
JOIN DWH.DIM_PRECIPITATION p ON cast(date_trunc('DAY', r.DATE) as DATE) = p.DATE
JOIN DWH.DIM_TEMPERATURE t ON cast(date_trunc('DAY', r.DATE) as DATE) = t.DATE
JOIN DWH.DIM_BUSINESS b ON r.BUSINESS_ID = b.BUSINESS_ID
AND p.PRECIPITATION != 999;

SELECT *
FROM DWH.FACT_CLIMATE_REVIEW
LIMIT 1000;

