USE DATABASE YELPWEATHER;

SELECT *
FROM YELPWEATHER.ODS.PRECIPITATION p
LIMIT 1000;

SELECT
    cast(date_trunc('DAY', date) as date) as date,
    business_id,
    avg(stars) as avg_stars,
    count(*) as num_reviews
FROM YELPWEATHER.ODS.YELP_REVIEW
GROUP BY date, business_id
ORDER BY date, business_id;

SELECT r.date, r.business_id, b.name, (t.min+t.max)/2 as average_temperature, p.precipitation, r.avg_stars as average_stars, r.num_reviews as review_count
FROM
    (SELECT
        cast(date_trunc('DAY', date) as date) as date,
        business_id,
        avg(stars) as avg_stars,
        count(*) as num_reviews
     FROM YELPWEATHER.ODS.YELP_REVIEW
     GROUP BY date, business_id
     ORDER BY date, business_id
    )AS r

JOIN YELPWEATHER.ODS.PRECIPITATION p ON r.date = p.date
JOIN YELPWEATHER.ODS.TEMPERATURE t ON r.date = t.date
JOIN YELPWEATHER.ODS.YELP_BUSINESS b ON r.business_id = b.business_id
AND p.precipitation != 999
ORDER BY r.date;