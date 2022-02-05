// Data Cleaning - Remove first row that are column name
DELETE FROM NY_PRECIPITATION WHERE DATE = 'date';

DELETE FROM NY_TEMPERATURE WHERE DATE = 'date';



// Data Cleaning
SELECT *
FROM STAGING.NY_PRECIPITATION
WHERE PRECIPITATION = 'T';

UPDATE STAGING.NY_PRECIPITATION
SET PRECIPITATION = '999'
WHERE PRECIPITATION = 'T';

CREATE OR REPLACE TABLE PRECIPITATION(
    date date,
    precipitation float,
    precipitation_normal float
);



// Precipitation
INSERT INTO PRECIPITATION SELECT * FROM STAGING.NY_PRECIPITATION;

SELECT *
FROM YELPWEATHER.ODS.PRECIPITATION
LIMIT 1000;



// Temperature
SELECT *
FROM YELPWEATHER.STAGING.NY_TEMPERATURE
LIMIT 1000;

CREATE OR REPLACE TABLE TEMPERATURE(
    date date,
    min float,
    max float,
    normal_min float,
    normal_max float
);

INSERT INTO TEMPERATURE SELECT * FROM YELPWEATHER.STAGING.NY_TEMPERATURE;

SELECT *
FROM YELPWEATHER.ODS.TEMPERATURE
LIMIT 1000;



// Business
SELECT *
FROM YELPWEATHER.STAGING.YELP_BUSINESS
LIMIT 100;

CREATE OR REPLACE TABLE YELP_BUSINESS (
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

INSERT INTO YELP_BUSINESS
    SELECT
    PARSE_JSON(USERJSON):business_id,
    PARSE_JSON(USERJSON):name,
    PARSE_JSON(USERJSON):address,
    PARSE_JSON(USERJSON):city,
    PARSE_JSON(USERJSON):state,
    PARSE_JSON(USERJSON):postal_code,
    PARSE_JSON(USERJSON):latitude,
    PARSE_JSON(USERJSON):longitude,
    PARSE_JSON(USERJSON):stars,
    PARSE_JSON(USERJSON):review_count,
    PARSE_JSON(USERJSON):is_open,
    PARSE_JSON(USERJSON):attributes,
    PARSE_JSON(USERJSON):categories,
    PARSE_JSON(USERJSON):hours
FROM YELPWEATHER.STAGING.YELP_BUSINESS



//Checkins
SELECT *
FROM YELPWEATHER.STAGING.YELP_CHECKIN;

CREATE OR REPLACE TABLE YELP_CHECKIN(
    business_id string,
    date string,
    constraint fk_business_id foreign key(business_id) references YELP_BUSINESS(business_id)
);

INSERT INTO YELP_CHECKIN
SELECT
    PARSE_JSON(USERJSON): business_id,
    PARSE_JSON(USERJSON): date
FROM YELPWEATHER.STAGING.YELP_CHECKIN;

SELECT *
FROM YELPWEATHER.ODS.YELP_CHECKIN
LIMIT 1000;



//Users
SELECT *
FROM YELPWEATHER.STAGING.YELP_USER;

CREATE OR REPLACE TABLE YELP_USER(
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
    constraint pk_user_id primary key(user_id)
);

INSERT INTO YELP_USER
SELECT
    PARSE_JSON(USERJSON):user_id,
    PARSE_JSON(USERJSON):name,
    PARSE_JSON(USERJSON):review_count,
    PARSE_JSON(USERJSON):yelping_since,
    PARSE_JSON(USERJSON):useful,
    PARSE_JSON(USERJSON):funny,
    PARSE_JSON(USERJSON):cool,
    PARSE_JSON(USERJSON):elite,
    PARSE_JSON(USERJSON):friends,
    PARSE_JSON(USERJSON):fans,
    PARSE_JSON(USERJSON):average_stars,
    PARSE_JSON(USERJSON):compliment_hot,
    PARSE_JSON(USERJSON):compliment_more,
    PARSE_JSON(USERJSON):compliment_profile,
    PARSE_JSON(USERJSON):compliment_cute,
    PARSE_JSON(USERJSON):compliment_list,
    PARSE_JSON(USERJSON):compliment_note,
    PARSE_JSON(USERJSON):compliment_plain,
    PARSE_JSON(USERJSON):compliment_cool,
    PARSE_JSON(USERJSON):compliment_funny,
    PARSE_JSON(USERJSON):compliment_writer,
    PARSE_JSON(USERJSON):compliment_photos
FROM YELPWEATHER.STAGING.YELP_USER;

SELECT *
FROM YELPWEATHER.ODS.YELP_USER
LIMIT 100;



//Reviews
SELECT *
FROM YELPWEATHER.STAGING.YELP_REVIEW
LIMIT 100;

CREATE OR REPLACE TABLE YELP_REVIEW(
    review_id string,
    user_id string,
    business_id string,
    stars float,
    userful number,
    funny number,
    cool number,
    text string,
    date timestamp_ntz,
    constraint pk_review_id primary key (review_id),
    constraint fk_user_id foreign key (user_id) references YELP_USER(user_id),
    constraint fk_business_id foreign key (business_id) references YELP_BUSINESS(business_id)
);

INSERT INTO YELP_REVIEW
SELECT
    PARSE_JSON(USERJSON):review_id,
    PARSE_JSON(USERJSON):user_id,
    PARSE_JSON(USERJSON):business_id,
    PARSE_JSON(USERJSON):stars,
    PARSE_JSON(USERJSON):useful,
    PARSE_JSON(USERJSON):funny,
    PARSE_JSON(USERJSON):cool,
    PARSE_JSON(USERJSON):text,
    PARSE_JSON(USERJSON):date
FROM YELPWEATHER.STAGING.YELP_REVIEW;

SELECT *
FROM YELPWEATHER.ODS.YELP_REVIEW
LIMIT 1000;



//Tips
SELECT *
FROM YELPWEATHER.STAGING.YELP_TIP
LIMIT 100;

CREATE OR REPLACE TABLE YELP_TIP(
    user_id string,
    business_id string,
    text string,
    date timestamp_ntz,
    compliment_count number,
    constraint fk_user_id foreign key(user_id) references YELP_USER(user_id),
    constraint fk_business_id foreign key(business_id) references YELP_BUSINESS(business_id)
);

INSERT INTO YELP_TIP
SELECT
    PARSE_JSON(USERJSON):user_id,
    PARSE_JSON(USERJSON):business_id,
    PARSE_JSON(USERJSON):text,
    PARSE_JSON(USERJSON):date,
    PARSE_JSON(USERJSON):compliment_count
FROM YELPWEATHER.STAGING.YELP_TIP;

SELECT *
FROM YELPWEATHER.ODS.YELP_TIP
LIMIT 100;



//Covid
SELECT *
FROM YELPWEATHER.STAGING.YELP_COVID
LIMIT 5;

CREATE OR REPLACE TABLE YELP_COVID(
    business_id string,
    highlights string,
    "delivery_or_takeout" boolean,
    "Grubhub_enabled" boolean,
    "Call_To_Action_enabled" boolean,
    "Request_a_Quote_Enabled" boolean,
    "Covid_Banner" string,
    "Temporary_Closed_Until" string,
    "Virtual_Services_Offered" string,
    foreign key(business_id) references YELP_BUSINESS(business_id)
);

INSERT INTO YELP_COVID
SELECT
    PARSE_JSON(USERJSON):business_id,
    PARSE_JSON(USERJSON):highlights,
    PARSE_JSON(USERJSON):"delivery_or_takeout",
    PARSE_JSON(USERJSON):"Grubhub enabled",
    PARSE_JSON(USERJSON):"Call To Action enabled",
    PARSE_JSON(USERJSON):"Request a Quote Enabled",
    PARSE_JSON(USERJSON):"Covid Banner",
    PARSE_JSON(USERJSON):"Temporary Closed Until",
    PARSE_JSON(USERJSON):"Virtual Services Offered"
FROM YELPWEATHER.STAGING.YELP_COVID;

SELECT *
FROM YELPWEATHER.ODS.YELP_COVID
LIMIT 100;