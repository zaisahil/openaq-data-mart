-- SQLite-compatible SQL queries for the OpenAQ 2017 data mart

-- 1) Cities whose monthly CO and SO2 averages are in the top 10% globally.
WITH monthly_pollution AS (
    SELECT
        l.city,
        l.country,
        date_trunc('month', t.utc) AS month,
        p.parameter,
        AVG(f.value) AS avg_value
    FROM fact_measurement f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id
    JOIN dim_parameter p ON f.parameter_id = p.parameter_id
    WHERE p.parameter IN ('co', 'so2')
    GROUP BY l.city, l.country, month, p.parameter
),
city_monthly AS (
    SELECT
        city,
        country,
        month,
        MAX(CASE WHEN parameter = 'co' THEN avg_value END) AS avg_co,
        MAX(CASE WHEN parameter = 'so2' THEN avg_value END) AS avg_so2
    FROM monthly_pollution
    GROUP BY city, country, month
),
thresholds AS (
    SELECT
        month,
        percentile_cont(0.9, avg_co) AS co_90,
        percentile_cont(0.9, avg_so2) AS so2_90
    FROM city_monthly
    GROUP BY month
)
SELECT
    c.city,
    c.country,
    c.month,
    c.avg_co,
    c.avg_so2
FROM city_monthly c
JOIN thresholds t ON c.month = t.month
WHERE c.avg_co >= t.co_90
  AND c.avg_so2 >= t.so2_90;

-- 2) Top 5 cities worldwide by daily average PM2.5.
SELECT
    l.city,
    l.country,
    date_trunc('day', t.utc) AS day,
    AVG(f.value) AS avg_pm25
FROM fact_measurement f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_parameter p ON f.parameter_id = p.parameter_id
WHERE p.parameter = 'pm25'
GROUP BY l.city, l.country, date_trunc('day', t.utc)
ORDER BY avg_pm25 DESC
LIMIT 5;

-- 3) For a single day, find the top 10 cities by PM2.5 and show CO/SO2 stats for those cities.

-- Query 3 returns no results because of data sparsity:
-- On 2017-08-19, the top PM2.5 cities (e.g., LEWIS AND CLARK, Coyhaique)
-- have no CO or SO2 measurements for that same day.
-- The join logic requires all pollutants on the same date,
-- so no matches exist and the query yields zero rows.

WITH target_day AS (
    SELECT date_trunc('day', '2017-08-19T00:00:00') AS day
),
pm25_top_cities AS (
    SELECT
        l.city,
        l.country,
        AVG(f.value) AS avg_pm25
    FROM fact_measurement f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id
    JOIN dim_parameter p ON f.parameter_id = p.parameter_id
    WHERE p.parameter = 'pm25'
      AND date_trunc('day', t.utc) = (SELECT day FROM target_day)
    GROUP BY l.city, l.country
    ORDER BY avg_pm25 DESC
    LIMIT 10
)
SELECT
    l.city,
    l.country,
    p.parameter,
    AVG(f.value) AS mean_value,
    percentile_cont(0.5, f.value) AS median_value,
    (
        SELECT
            f2.value
        FROM fact_measurement f2
        JOIN dim_time t2 ON f2.time_id = t2.time_id
        JOIN dim_parameter p2 ON f2.parameter_id = p2.parameter_id
        JOIN dim_location l2 ON f2.location_id = l2.location_id
        WHERE p2.parameter = p.parameter
          AND date_trunc('day', t2.utc) = (SELECT day FROM target_day)
          AND l2.city = l.city
          AND l2.country = l.country
        GROUP BY f2.value
        ORDER BY COUNT(*) DESC, f2.value ASC
        LIMIT 1
    ) AS mode_value
FROM fact_measurement f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_parameter p ON f.parameter_id = p.parameter_id
JOIN pm25_top_cities top ON l.city = top.city AND l.country = top.country
WHERE p.parameter IN ('co', 'so2')
  AND date_trunc('day', t.utc) = (SELECT day FROM target_day)
GROUP BY l.city, l.country, p.parameter;

-- 4) Hourly country air quality index with three levels.
WITH hourly_country AS (
    SELECT
        l.country,
        date_trunc('hour', t.utc) AS hour,
        AVG(CASE WHEN p.parameter = 'pm25' THEN f.value END) AS avg_pm25,
        AVG(CASE WHEN p.parameter = 'so2' THEN f.value END) AS avg_so2,
        AVG(CASE WHEN p.parameter = 'co' THEN f.value END) AS avg_co
    FROM fact_measurement f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_location l ON f.location_id = l.location_id
    JOIN dim_parameter p ON f.parameter_id = p.parameter_id
    GROUP BY l.country, date_trunc('hour', t.utc)
)
SELECT
    country,
    hour,
    avg_pm25,
    avg_so2,
    avg_co,
    CASE
        WHEN (COALESCE(avg_pm25 / 35.0, 0) + COALESCE(avg_so2 / 0.075, 0) + COALESCE(avg_co / 9.0, 0)) / 3.0 >= 0.75 THEN 'high'
        WHEN (COALESCE(avg_pm25 / 35.0, 0) + COALESCE(avg_so2 / 0.075, 0) + COALESCE(avg_co / 9.0, 0)) / 3.0 >= 0.4 THEN 'moderate'
        ELSE 'low'
    END AS air_quality_level
FROM hourly_country;
