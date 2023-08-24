Odynn - sql queries to create datafile

---- CREATE CLEAN TABLES ---- 
---- cash ---- 
DROP TABLE IF EXISTS hotel_cash;
CREATE TABLE hotel_cash AS

WITH first_table as (
	SELECT *, date(created_at) as date_scraped,
		-- excluded award_category to ensure no dupes for a given hotel, date_booking, date_scraped. Check if one award_category is 'better'?
		row_number() OVER (PARTITION BY hotel_group, hotel_name_key, date_booking, date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_cash_raw
	WHERE currency = 'USD' and cash_value > 1
		AND date(created_at) <= date_booking -- remove booking dates in the past
)
SELECT hotel_group, hotel_name_key, city, date(date_booking) as date_booking, cash_value as cash, currency, date_scraped, created_at, _id
FROM first_table
WHERE rn = 1;

---- points ---- 
DROP TABLE IF EXISTS hotel_points;
CREATE TABLE hotel_points AS

WITH first_table as (
	SELECT *, date(created_at) as date_scraped,
	-- excluded points_category and points_level to ensure no dupes for a given hotel, date_booking, date_scraped. Is one category is 'better'?
		row_number() OVER (PARTITION BY hotel_group, hotel_name_key, date_booking, date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_award_raw
	WHERE points > 1
		AND date(created_at) <= date_booking -- remove booking dates in the past
)
SELECT hotel_group, hotel_name_key, city, state, country, date(date_booking) as date_booking, points, date_scraped, created_at, _id
FROM first_table
WHERE rn = 1;

-- COMBINE CLEAN TABLES ---
DROP TABLE IF EXISTS hotel;
CREATE TABLE hotel AS

WITH dates as ( -- list of dates starting 
	SELECT
		date(date_scraped) as date_scraped,
	    date(generate_series(date_scraped, date_scraped + interval '1 year' - interval '1 day', interval '1 day')) AS date_booking
	FROM (
	    SELECT generate_series('2022-11-15'::date, CURRENT_DATE, '1 day'::interval) AS date_scraped
	) as date_scraped
)
SELECT a.date_scraped, a.date_booking,
	a.date_booking - a.date_scraped as days_in_advance,
	b.hotel_group, b.city, b.hotel_name_key,
	c.cash, d.points
FROM dates a
CROSS JOIN (SELECT distinct hotel_group, hotel_name_key, city FROM hotel_cash) b -- create array of potential date_scraped x booking_date x hotel_name_key to easily analyze missing values
LEFT JOIN hotel_cash c USING (date_scraped, date_booking, hotel_name_key)
LEFT JOIN hotel_points d USING (date_scraped, date_booking, hotel_name_key)
ORDER BY hotel_group, city, hotel_name_key, date_booking, date_scraped;

-- CREATE hotel_dense - remove hotel rows where both cash and points are null (returns ~1.4m rows)
DROP TABLE IF EXISTS hotel_dense;
CREATE TABLE hotel_dense as

SELECT *
FROM hotel
WHERE cash IS NOT NULL OR points IS NOT NULL
ORDER BY hotel_group, hotel_name_key, date_booking, date_scraped;

-- [to delete] CREATE hotel_best - best hotel from each group
CREATE TABLE hotel_best as
SELECT *
FROM hotel_dense
WHERE  hotel_name_key IN (
	'hilton-club-west-57th-street-new-york',
	'hyatt-house-new-york-chelsea',
	'holiday-inn-manhattan-financial-district',
	'delta-hotels-new-york-times-square')
ORDER BY hotel_group, hotel_name_key, date_booking, date_scraped;


-- INTERPOLATE ESTIMATED CASH / POINTS VALUES
WITH cash_prev_cte as (
	SELECT hotel_group, city, hotel_name_key, date_booking, date_scraped, days_in_advance, cash, points,
		cash_prev, date_scraped - date_scraped_prev as cp_gap--, date_scraped_prev, rn
	FROM (
		SELECT a.*,
			b.cash as cash_prev, b.date_scraped as date_scraped_prev,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_booking, a.date_scraped ORDER BY b.date_scraped DESC) rn
		FROM hotel_sample_base a
		LEFT JOIN hotel_cash b
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_booking = b.date_booking
			AND a.date_scraped > b.date_scraped -- Only consider prior dates of scraping
		WHERE a.hotel_name_key = 'distrikt-hotel-new-york-city-tapestry-collection-by-hilton' -- TEMP
	) c
	WHERE rn = 1
)
,cash_next_cte as (
	SELECT hotel_group, city, hotel_name_key, date_booking, date_scraped, days_in_advance, cash, points,
		cash_prev, cp_gap,
		cash_next, date_scraped_next - date_scraped as cn_gap--, date_scraped_next, rn
	FROM (
		SELECT 	a.*, b.cash as cash_next, b.date_scraped as date_scraped_next,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_booking, a.date_scraped ORDER BY b.date_scraped ASC) rn
		FROM cash_prev_cte a
		LEFT JOIN hotel_cash b
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_booking = b.date_booking
			AND a.date_scraped < b.date_scraped -- Only consider future dates of scraping
	) c
	WHERE rn = 1
)
, points_prev_cte as (
	SELECT hotel_group, city, hotel_name_key, date_booking, date_scraped, days_in_advance, cash, points,
		cash_prev, cp_gap, cash_next, cn_gap,
		points_prev, date_scraped - date_scraped_prev as pp_gap--, date_scraped_prev, rn
	FROM (
		SELECT a.*,
			b.points as points_prev, b.date_scraped as date_scraped_prev,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_booking, a.date_scraped ORDER BY b.date_scraped DESC) rn
		FROM cash_next_cte a
		LEFT JOIN hotel_points b
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_booking = b.date_booking
			AND a.date_scraped > b.date_scraped -- Only consider prior dates of scraping
	) c
	WHERE rn = 1
)
,points_next_cte as (
	SELECT hotel_group, city, hotel_name_key, date_booking, date_scraped, days_in_advance, cash, points,
		cash_prev, cp_gap, cash_next, cn_gap, points_prev, pp_gap,
		points_next, date_scraped_next - date_scraped as pn_gap--, date_scraped_next, rn
	FROM (
		SELECT 	a.*, b.points as points_next, b.date_scraped as date_scraped_next,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_booking, a.date_scraped ORDER BY b.date_scraped ASC) rn
		FROM points_prev_cte a
		LEFT JOIN hotel_points b
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_booking = b.date_booking
			AND a.date_scraped < b.date_scraped -- Only consider future dates of scraping
	) c
	WHERE rn = 1
)
SELECT hotel_group, city, hotel_name_key, date_booking, date_scraped, days_in_advance, cash, points,
		cash_prev, cp_gap, cash_next, cn_gap, points_prev, pp_gap,
		points_next, pn_gap
FROM points_next_cte
ORDER BY hotel_group, hotel_name_key, date_booking, days_in_advance, date_scraped;




-- (not working) CREATE hotel_interp - estimate missing cash and points based on surrounding date_scraped for a given date_booking
DROP TABLE IF EXISTS hotel;
CREATE TABLE hotel_interp AS

WITH dates AS (
    SELECT
        date(date_scraped) AS date_scraped,
        date(generate_series(date_scraped, date_scraped + interval '1 year' - interval '1 day', interval '1 day')) AS date_booking
    FROM (
        SELECT generate_series('2022-11-15'::date, CURRENT_DATE, '1 day'::interval) AS date_scraped
    ) AS date_scraped
),
base AS (
    SELECT 
        a.date_scraped as dt, a.date_booking,
        b.city, b.hotel_group, b.hotel_name_key,
        c.cash_value AS cash, d.points
    FROM dates a
    CROSS JOIN (SELECT DISTINCT hotel_group, hotel_name_key, city FROM hotel_cash) b
    LEFT JOIN hotel_cash c USING (date_scraped, date_booking, hotel_name_key)
    LEFT JOIN hotel_points d USING (date_scraped, date_booking, hotel_name_key)
),
base_with_bounds AS (
    SELECT
        *,
        LAG(cash) OVER w AS prev_cash,
        LEAD(cash) OVER w AS next_cash,
        LAG(dt) OVER w AS prev_dt_cash,
        LEAD(dt) OVER w AS next_dt_cash,
        LAG(points) OVER w AS prev_points,
        LEAD(points) OVER w AS next_points,
        LAG(dt) OVER w AS prev_dt_points,
        LEAD(dt) OVER w AS next_dt_points
    FROM base
    WINDOW w AS (
        PARTITION BY hotel_group, hotel_name_key, date_booking
        ORDER BY dt
    )
),
interpolated AS (
    SELECT 
        base_with_bounds.*,
        COALESCE(
            cash,
            CASE 
                WHEN prev_cash IS NOT NULL AND next_cash IS NOT NULL THEN
                    (prev_cash + (next_cash - prev_cash) * (dt - prev_dt_cash) / (next_dt_cash - prev_dt_cash))::NUMERIC(10, 2)
                ELSE 
                    COALESCE(prev_cash, next_cash)
            END
        ) AS est_cash,
        COALESCE(
            points,
            CASE 
                WHEN prev_points IS NOT NULL AND next_points IS NOT NULL THEN
                    ROUND(prev_points + (next_points - prev_points) * (dt - prev_dt_points) / (next_dt_points - prev_dt_points))
                ELSE 
                    COALESCE(prev_points, next_points)
            END
        ) AS est_points,
        CASE 
            WHEN cash IS NULL 
            THEN LEAST(dt - prev_dt_cash, next_dt_cash - dt) 
            ELSE 0 
        END AS est_cash_gap,
        CASE 
            WHEN points IS NULL 
            THEN LEAST(dt - prev_dt_points, next_dt_points - dt) 
            ELSE 0 
        END AS est_points_gap
    FROM base_with_bounds
)

SELECT 
    interpolated.*,
    CASE WHEN interpolated.cash IS NOT NULL THEN ABS(interpolated.est_cash - interpolated.cash) / interpolated.cash END AS est_cash_error,
    CASE WHEN interpolated.points IS NOT NULL THEN ABS(interpolated.est_points - interpolated.points) / interpolated.points END AS est_points_error
FROM interpolated;


-- How many future booking days are populated for week of scraping, by day of week scraped
SELECT hotel_group, hotel_name_key,
	date_trunc('week', date_scraped) as week_scraped,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 0 THEN cash_value END) as sun_cash,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 1 THEN cash_value END) as mon_cash,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 2 THEN cash_value END) as tue_cash,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 3 THEN cash_value END) as wed_cash,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 4 THEN cash_value END) as thu_cash,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 5 THEN cash_value END) as fri_cash,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 6 THEN cash_value END) as sat_cash,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 0 THEN points END) as sun_points,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 1 THEN points END) as mon_points,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 2 THEN points END) as tue_points,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 3 THEN points END) as wed_points,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 4 THEN points END) as thu_points,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 5 THEN points END) as fri_points,
	count(CASE WHEN EXTRACT(DOW FROM date_scraped) = 6 THEN points END) as sat_points
FROM hotel
GROUP BY 1,2,3
ORDER BY 1,3,2;

--By hotel and scraping week, how many future booking_date's have both cash and points values, as of only Sunday, Sunday - Monday ... Sunday-Saturday
WITH distinct_bookings_cash AS (
    SELECT 
        hotel_name_key, 
        hotel_group,
        date_booking, 
        DATE_TRUNC('week', date_scraped) AS week_scraped,
        MIN(date_scraped) AS date_scraped
    FROM 
        hotel
    WHERE cash_value IS NOT NULL
    GROUP BY
        hotel_name_key, 
        hotel_group,
        date_booking, 
        week_scraped
),
distinct_bookings_points AS (
    SELECT 
        hotel_name_key, 
        hotel_group,
        date_booking, 
        DATE_TRUNC('week', date_scraped) AS week_scraped,
        MIN(date_scraped) AS date_scraped
    FROM 
        hotel
    WHERE points IS NOT NULL
    GROUP BY
        hotel_name_key, 
        hotel_group,
        date_booking, 
        week_scraped
),
distinct_bookings_both AS (
    SELECT 
        cash.hotel_name_key, 
        cash.hotel_group,
        cash.date_booking, 
        cash.week_scraped,
        GREATEST(cash.date_scraped, points.date_scraped) AS date_scraped
    FROM 
        distinct_bookings_cash AS cash
    JOIN 
        distinct_bookings_points AS points
    ON 
        cash.hotel_name_key = points.hotel_name_key AND
        cash.hotel_group = points.hotel_group AND
        cash.date_booking = points.date_booking AND
        cash.week_scraped = points.week_scraped
)

SELECT
    cp.hotel_group,
    cp.hotel_name_key,
    cp.week_scraped,
    COUNT(cp.date_scraped) FILTER (WHERE EXTRACT(DOW FROM cp.date_scraped) = 0) AS d1_cp,
    COUNT(cp.date_scraped) FILTER (WHERE EXTRACT(DOW FROM cp.date_scraped) IN (0, 1)) AS d2_cp,
    COUNT(cp.date_scraped) FILTER (WHERE EXTRACT(DOW FROM cp.date_scraped) IN (0, 1, 2)) AS d3_cp,
    COUNT(cp.date_scraped) FILTER (WHERE EXTRACT(DOW FROM cp.date_scraped) IN (0, 1, 2, 3)) AS d4_cp,
    COUNT(cp.date_scraped) FILTER (WHERE EXTRACT(DOW FROM cp.date_scraped) IN (0, 1, 2, 3, 4)) AS d5_cp,
    COUNT(cp.date_scraped) FILTER (WHERE EXTRACT(DOW FROM cp.date_scraped) IN (0, 1, 2, 3, 4, 5)) AS d6_cp,
    COUNT(cp.date_scraped) FILTER (WHERE EXTRACT(DOW FROM cp.date_scraped) IN (0, 1, 2, 3, 4, 5, 6)) AS d7_cp
FROM
    distinct_bookings_both AS cp
GROUP BY
    cp.hotel_name_key,
    cp.hotel_group,
    cp.week_scraped
ORDER BY
    cp.hotel_group,
    cp.hotel_name_key,
    cp.week_scraped;