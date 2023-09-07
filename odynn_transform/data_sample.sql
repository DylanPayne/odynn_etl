-- V2 - Odynn TABLE CREATION FOR FORBES SAMPLE --

---- CREATE CLEAN TABLES ---- 
---- 1. cash ---- 
DROP TABLE IF EXISTS hotel_cash;
CREATE TABLE hotel_cash AS
WITH new_york as(
	SELECT hotel_group, hotel_id
	FROM hotel_templates
	where slug_city = 'new-york'
)
,cte as (
	SELECT *, date(created_at) as date_booked,
		row_number() OVER (PARTITION BY hotel_group, hotel_name_key, date(date), date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_cash_raw a
	INNER JOIN new_york b
	USING (hotel_group, hotel_id)
	WHERE currency = 'USD' and cash_value > 1
		AND date(created_at) <= date(date) -- remove booking dates in the past
		AND NULLIF(hotel_name_key,'') IS NOT NULL
	
)
SELECT hotel_group, hotel_name_key, hotel_id, date(date) as date_stay, date_booked, cash_value as cash, currency, created_at, _id
FROM cte
WHERE rn = 1;

---- 2. points ---- 
DROP TABLE IF EXISTS hotel_points;
CREATE TABLE hotel_points AS
WITH new_york as(
	SELECT hotel_group, hotel_id
	FROM hotel_templates
	where slug_city = 'new-york'
),
cte as (
	SELECT *, date(created_at) as date_booked,
		row_number() OVER (PARTITION BY hotel_group, hotel_name_key, date(date), date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_points_raw a
	INNER JOIN new_york b
	USING (hotel_group, hotel_id)
	WHERE points > 1
		AND date(created_at) <= date(date) -- remove booking dates in the past
		AND NULLIF(hotel_name_key,'') IS NOT NULL
)
SELECT hotel_group, hotel_name_key, hotel_id, date(date) as date_stay, date_booked, points, created_at, _id
FROM cte
WHERE rn = 1;

-- 3. COMBINE CLEAN TABLES into HOTEL ---
DROP TABLE IF EXISTS hotel;
CREATE TABLE hotel AS
WITH dates as ( -- list of dates starting 
	SELECT
		date(date_booked) as date_booked,
	    date(generate_series(date_booked, date_booked + interval '1 year' - interval '1 day', interval '1 day')) AS date_stay
	FROM (
	    SELECT generate_series('2022-11-15'::date, CURRENT_DATE, '1 day'::interval) AS date_booked
	) as date_booked
)
SELECT a.date_booked, a.date_stay,
	a.date_stay - a.date_booked as booking_window,
	b.hotel_group, b.hotel_name_key,
	c.cash, d.points
FROM dates a
CROSS JOIN (SELECT distinct hotel_group, hotel_name_key FROM hotel_cash) b -- create array of potential date_booked x booking_date x hotel_name_key to easily analyze missing values
LEFT JOIN hotel_cash c USING (date_booked, date_stay, hotel_name_key, hotel_group)
LEFT JOIN hotel_points d USING (date_booked, date_stay, hotel_name_key, hotel_group)
ORDER BY hotel_group, hotel_name_key, date_stay, date_booked;

-- BAD - Redesigned for speed, may not keep all possible rows for desired booking windows + date/hotel combos with points or cash data
CREATE TABLE hotel AS
WITH specific_dates AS (
    SELECT
        b.hotel_group, b.hotel_name_key,
        date_booked,
        unnest(array[
            date_booked + interval '3 day',
            date_booked + interval '7 day',
            date_booked + interval '14 day',
            date_booked + interval '30 day',
            date_booked + interval '60 day',
            date_booked + interval '90 day',
            date_booked + interval '180 day'
        ]) AS date_stay
    FROM (
        SELECT date(generate_series('2022-12-01'::date, CURRENT_DATE, interval '1 day')) AS date_booked
    ) AS cte
    CROSS JOIN (SELECT DISTINCT hotel_group, hotel_name_key FROM hotel_cash) b
),
matching_dates AS (
    SELECT c.hotel_group, c.hotel_name_key, c.date_booked, c.date_stay
    FROM (
        SELECT a.hotel_group, a.hotel_name_key, a.date_stay, a.date_booked
        FROM hotel_cash a
        UNION
        SELECT b.hotel_group, b.hotel_name_key, b.date_stay, b.date_booked
        FROM hotel_points b
    ) AS c
)
SELECT a.hotel_group, a.hotel_name_key,
    a.date_stay, a.date_booked,
	EXTRACT(DAY FROM (a.date_stay - a.date_booked))::integer AS booking_window,
    c.cash, d.points
FROM (
    SELECT * FROM specific_dates
    UNION
    SELECT * FROM matching_dates
) a
LEFT JOIN hotel_cash c USING (hotel_group, hotel_name_key, date_stay, date_booked)
LEFT JOIN hotel_points d USING (hotel_group, hotel_name_key, date_stay, date_booked)
ORDER BY hotel_group, hotel_name_key, date_stay, date_booked;

-- 3.5 Index to hotel, 3 min 
DROP INDEX IF EXISTS idx_hotel_keys;
CREATE INDEX idx_hotel_keys ON hotel(hotel_group, hotel_name_key, date_stay, date_booked);

-- 4. Create hotel_sample_base and index for speed
DROP TABLE IF EXISTS hotel_sample_base;
CREATE TABLE hotel_sample_base as
SELECT *
FROM hotel
WHERE booking_window IN (3, 7, 14, 30, 60, 90, 180);
CREATE INDEX idx_hotel_sample_base_key ON hotel_sample_base(hotel_name_key);
CREATE INDEX idx_hotel_sample_base_group ON hotel_sample_base(hotel_group);
CREATE INDEX idx_hotel_sample_base_stay ON hotel_sample_base(date_stay);
CREATE INDEX idx_hotel_sample_base_booked ON hotel_sample_base(date_booked);


-- 5. CREATE ESTIMATED CASH + POINTS TABLE VIA INTERPOLATION
-- 37 min with indexes, 163 new-york branches, hotel v2
-- 1,163,694	158,700	179,474
SELECT count(*), sum(CASE WHEN cash IS NOT NULL THEN 1 END) as cash, sum(CASE WHEN points IS NOT NULL THEN 1 END) as points from hotel_sample_est;
DROP TABLE IF EXISTS hotel_sample_est;
CREATE TABLE hotel_sample_est as
WITH cash_prev_cte as (
	SELECT hotel_group, hotel_name_key, date_stay, date_booked, booking_window, cash, points,
		cash_prev, date_booked - date_booked_prev as cp_gap--, date_booked_prev, rn
	FROM (
		SELECT a.*,
			b.cash as cash_prev, b.date_booked as date_booked_prev,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_stay, a.date_booked ORDER BY b.date_booked DESC) rn
		FROM hotel_sample_base a
		LEFT JOIN hotel_cash b -- Join cash-only table with no NULLs to ensure rn = 1 refers to adjacent valid observation
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_stay = b.date_stay
			AND a.date_booked > b.date_booked -- Only consider prior dates of scraping
	) c
	WHERE rn = 1
)
,cash_next_cte as (
	SELECT hotel_group, hotel_name_key, date_stay, date_booked, booking_window, cash, points,
		cash_prev, cp_gap,
		cash_next, date_booked_next - date_booked as cn_gap--, date_booked_next, rn
	FROM (
		SELECT 	a.*, b.cash as cash_next, b.date_booked as date_booked_next,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_stay, a.date_booked ORDER BY b.date_booked ASC) rn
		FROM cash_prev_cte a
		LEFT JOIN hotel_cash b
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_stay = b.date_stay
			AND a.date_booked < b.date_booked -- Only consider future dates of scraping
	) c
	WHERE rn = 1
)
, points_prev_cte as (
	SELECT hotel_group, hotel_name_key, date_stay, date_booked, booking_window, cash, points,
		cash_prev, cp_gap, cash_next, cn_gap,
		points_prev, date_booked - date_booked_prev as pp_gap--, date_booked_prev, rn
	FROM (
		SELECT a.*,
			b.points as points_prev, b.date_booked as date_booked_prev,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_stay, a.date_booked ORDER BY b.date_booked DESC) rn
		FROM cash_next_cte a
		LEFT JOIN hotel_points b
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_stay = b.date_stay
			AND a.date_booked > b.date_booked -- Only consider prior dates of scraping
	) c
	WHERE rn = 1
)
,points_next_cte as (
	SELECT hotel_group, hotel_name_key, date_stay, date_booked, booking_window, cash, points,
		cash_prev, cp_gap, cash_next, cn_gap, points_prev, pp_gap,
		points_next, date_booked_next - date_booked as pn_gap--, date_booked_next, rn
	FROM (
		SELECT 	a.*, b.points as points_next, b.date_booked as date_booked_next,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_stay, a.date_booked ORDER BY b.date_booked ASC) rn
		FROM points_prev_cte a
		LEFT JOIN hotel_points b
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_stay = b.date_stay
			AND a.date_booked < b.date_booked -- Only consider future dates of scraping
	) c
	WHERE rn = 1
)
SELECT hotel_group, hotel_name_key, date_stay, date_booked, booking_window, cash, points,
		ROUND(CASE 	WHEN cash_prev IS NOT NULL AND cash_next IS NOT NULL THEN cash_prev + ((cash_next - cash_prev) * cp_gap * 1.0/(cp_gap+cn_gap)) -- interpolate if both non-Null
					WHEN cash_prev IS NULL and cash_next IS NULL THEN NULL -- both null
					ELSE COALESCE(cash_prev, cash_next) -- If one is Null, use the non-Null value
			END::NUMERIC,2) as cash_est,
		ROUND(CASE 	WHEN points_prev IS NOT NULL AND points_next IS NOT NULL THEN points_prev + ((points_next - points_prev) * pp_gap * 1.0/(pp_gap+pn_gap)) -- both
			WHEN points_prev IS NULL and points_next IS NULL THEN NULL -- neither
			ELSE COALESCE(points_prev, points_next) -- only one is non-NULL
		END::NUMERIC,0) as points_est,
		cash_prev, cp_gap, cash_next, cn_gap, points_prev, pp_gap,
		points_next, pn_gap
FROM points_next_cte
ORDER BY hotel_group, hotel_name_key, date_stay, booking_window, date_booked;

-- 6. CREATE hotel_sample --
DROP TABLE IF EXISTS hotel_sample;
CREATE TABLE hotel_sample as
SELECT hotel_group, hotel_name_key, date_stay, date_booked, booking_window,
	COALESCE(cash, cash_est) as cash, COALESCE(points, points_est) as points,
	CASE WHEN COALESCE(cash, cash_est) IS NOT NULL AND COALESCE(points, points_est) IS NOT NULL THEN TRUE ELSE FALSE END as complete
FROM hotel_sample_est
WHERE date_booked BETWEEN '2022-12-01' AND '2023-07-31'
	AND hotel_name_key NOT IN (
''
)
ORDER BY 1,2,3,4,5;

-- Output restricted sample, 10 hotels per group
SELECT *
FROM hotel_sample
WHERE hotel_name_key IN (
-- marriott
	'fairfield-inn-suites-new-york-manhattan-times-square',
	'fairfield-inn-suites-new-york-manhattan-chelsea',
	'fairfield-inn-suites-new-york-manhattan-fifth-avenue'
	'residence-inn-new-york-manhattan-midtown-east',
	'renaissance-new-york-midtown-hotel',
	'jw-marriott-essex-house-new-york',
	'the-algonquin-hotel-times-square-autograph-collection',
	'the-ritz-carlton-new-york-nomad',
	'courtyard-new-york-manhattan-times-square',
	'courtyard-new-york-manhattan-midtown-east',
-- ihg
	'holiday-inn-express-manhattan-midtown-west',
	'crowne-plaza-hy36-midtown-manhattan',
	'hotel-indigo-lower-east-side-new-york',
	'holiday-inn-express-new-york-city-times-square',
	'crowne-plaza-times-square-manhattan',
	'even-hotels-new-york-times-square-south',
	'holiday-inn-express-new-york-city-wall-street',
	'holiday-inn-new-york-city-wall-street',
	'holiday-inn-express-queens-maspeth',
	'holiday-inn-express-jamaica-jfk-airtrain-nyc',
-- hilton
	'hilton-garden-inn-new-york-central-park-south-midtown-west',
	'distrikt-hotel-new-york-city-tapestry-collection-by-hilton',
	'hilton-garden-inn-new-york-midtown-park-ave',
	'hilton-garden-inn-new-york-tribeca',
	'doubletree-by-hilton-hotel-new-york-times-square-west',
	'hilton-new-york-times-square',
	'hampton-inn-manhattan-times-square-central',
	'new-york-hilton-midtown',
	'hampton-inn-manhattan-times-square-south',
	'hilton-garden-inn-new-york-times-square-central',
-- hyatt	
	'park-hyatt-new-york',
	'grayson-hotel',
	'thompson-central-park-new-york',
	'hyatt-union-square-new-york',
	'hyatt-regency-jfk-airport-at-resorts-world-new-york',
	'hyatt-place-new-york-yonkers',
	'hyatt-place-new-york-chelsea',
	'hyatt-house-new-york-chelsea',
	'hyatt-centric-wall-street-new-york',
	'the-beekman-a-thompson-hotel'
)
ORDER BY 1,2,3,4

-- Odynn - sql queries to create datafile v 1

---- ### CREATE CLEAN TABLES ### ---- 
---- 1. hotel_cash ---- 
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

---- 2. hotel_points ---- 
DROP TABLE IF EXISTS hotel_points;
CREATE TABLE hotel_points AS

WITH first_table as (
	SELECT *, date(created_at) as date_scraped,
	-- excluded points_category and points_level to ensure no dupes for a given hotel, date_booking, date_scraped. Is one category is 'better'?
		row_number() OVER (PARTITION BY hotel_group, hotel_name_key, date_booking, date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_points_raw
	WHERE points > 1
		AND date(created_at) <= date_booking -- remove booking dates in the past
)
SELECT hotel_group, hotel_name_key, city, state, country, date(date_booking) as date_booking, points, date_scraped, created_at, _id
FROM first_table
WHERE rn = 1;

-- 3. COMBINE CLEAN TABLES ---
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


-- 4. CREATE ESTIMATED CASH + POINTS TABLE VIA INTERPOLATION
DROP TABLE IF EXISTS hotel_sample_est;
CREATE TABLE hotel_sample_est as
WITH hotel_sample_base as (
	SELECT *
	FROM hotel
	WHERE days_in_advance IN (3, 7, 14, 30, 60, 90, 180);
),
cash_prev_cte as (
	SELECT hotel_group, city, hotel_name_key, date_booking, date_scraped, days_in_advance, cash, points,
		cash_prev, date_scraped - date_scraped_prev as cp_gap--, date_scraped_prev, rn
	FROM (
		SELECT a.*,
			b.cash as cash_prev, b.date_scraped as date_scraped_prev,
			ROW_NUMBER() OVER(PARTITION BY a.hotel_group, a.hotel_name_key, a.date_booking, a.date_scraped ORDER BY b.date_scraped DESC) rn
		FROM hotel_sample_base a
		LEFT JOIN hotel_cash b -- Join cash-only table with no NULLs to ensure rn = 1 refers to adjacent valid observation
		ON a.hotel_group = b.hotel_group AND a.hotel_name_key = b.hotel_name_key AND a.date_booking = b.date_booking
			AND a.date_scraped > b.date_scraped -- Only consider prior dates of scraping
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
		ROUND(CASE 	WHEN cash_prev IS NOT NULL AND cash_next IS NOT NULL THEN cash_prev + ((cash_next - cash_prev) * cp_gap * 1.0/(cp_gap+cn_gap)) -- interpolate if both non-Null
					WHEN cash_prev IS NULL and cash_next IS NULL THEN NULL -- both null
					ELSE COALESCE(cash_prev, cash_next) -- If one is Null, use the non-Null value
			END::NUMERIC,2) as cash_est,
		ROUND(CASE 	WHEN points_prev IS NOT NULL AND points_next IS NOT NULL THEN points_prev + ((points_next - points_prev) * pp_gap * 1.0/(pp_gap+pn_gap)) -- both
			WHEN points_prev IS NULL and points_next IS NULL THEN NULL -- neither
			ELSE COALESCE(points_prev, points_next) -- only one is non-NULL
		END::NUMERIC,0) as points_est,
		cash_prev, cp_gap, cash_next, cn_gap, points_prev, pp_gap,
		points_next, pn_gap
FROM points_next_cte
ORDER BY hotel_group, hotel_name_key, date_booking, days_in_advance, date_scraped;

-- 5. CREATE hotel_sample --
DROP TABLE IF EXISTS hotel_sample;
CREATE TABLE hotel_sample as
SELECT city, hotel_group, hotel_name_key, date_booking, date_scraped, days_in_advance,
	COALESCE(cash, cash_est) as cash, COALESCE(points, points_est) as points,
	CASE WHEN COALESCE(cash, cash_est) IS NOT NULL AND COALESCE(points, points_est) IS NOT NULL THEN TRUE ELSE FALSE END as complete
FROM hotel_sample_est
WHERE date_scraped BETWEEN '2022-12-01' AND '2023-07-31'
	AND hotel_name_key NOT IN (
--	'the-times-square-edition',
--	'andaz-5th-avenue',
--	'gild-hall-a-thompson-hotel',
	'hilton-club-the-quin-new-york',
	'hilton-club-west-57th-street-new-york',
	'delta-hotels-new-york-times-square',
	'intercontinental-hotels-new-york-barclay', -- This takes ihg down to 9 hotels, but this one only has data since June!
	'ihg-kimpton-hotel-theta'
)
ORDER BY 1,2,3,4,5;

-- 7. Dedupe templates to latest observation per hotel_id (currently not needed, since extracting from  "current" table)
SELECT *
FROM (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY hotel_group, hotel_id ORDER BY created_at DESC) as rn
	FROM hotel_templates
	WHERE hotel_group is not null and hotel_id is not null AND hotel_name is NOT NULL
) a
WHERE rn = 1

-- 8. Latest Templates for 40 in-sample hotel branches
DROP TABLE IF EXISTS hotel_sample_40_templates;
CREATE TABLE hotel_sample_40_templates as
WITH deduped as (
	SELECT *
	FROM (
		SELECT *,
			ROW_NUMBER() OVER(PARTITION BY hotel_group, hotel_id ORDER BY created_at DESC) as rn
		FROM hotel_templates
		WHERE hotel_group is not null and hotel_id is not null AND hotel_name is NOT NULL
	) a
	WHERE rn = 1
),
map_table as (
	SELECT *
	FROM (
		SELECT hotel_group, hotel_id, hotel_name_key, row_number() OVER(PARTITION BY hotel_group, hotel_id ORDER BY created_at DESC) as rn
		FROM hotel_cash
		WHERE hotel_group IS NOT NULL AND hotel_id IS NOT NULL and hotel_name_key IS NOT NULL
	) a
	WHERE rn = 1
),
hotel_40 as (
	SELECT distinct hotel_group, hotel_name_key
	FROM hotel_sample_40
)
,sample_hotel_ids as (
	SELECT distinct a.hotel_group, a.hotel_id, b.hotel_name_key
	FROM map_table a
	INNER JOIN hotel_40 b USING (hotel_group, hotel_name_key)
)
SELECT hotel_group, hotel_id, hotel_name_key, hotel_name, description, address, city, slug_city, state, state_code, latitude, longitude, currency, review_count, review_rating, country, country_code, telephone, chain_rating
FROM deduped a
INNER JOIN sample_hotel_ids USING (hotel_group, hotel_id)
ORDER BY hotel_group, hotel_name_key;

---- ### CREATE CLEAN TABLES ### ---- 

-- #### QA for ingestion ### --

-- Dupes check - cash and points
SELECT _id, count(*)
FROM hotel_cash_raw
GROUP BY 1
HAVING count(*) > 1
ORDER BY 2 desc;
SELECT _id, count(*)
FROM hotel_points_raw
GROUP BY 1
HAVING count(*) > 1
ORDER BY 2 desc;

-- Rows and max/min created_at for each hotel branch - cash and points
SELECT hotel_group, hotel_name_key, hotel_name_key, count(*), min(created_at) as min, max(created_at) as max
FROM hotel_cash_raw
GROUP BY 1,2
ORDER BY 1,4 desc;
SELECT hotel_group, hotel_name_key, hotel_name_key, count(*), min(created_at) as min, max(created_at) as max
FROM hotel_points_raw
GROUP BY 1,2
ORDER BY 1,4 desc;

-- ### QA for interpolation accuracy ### --

-- Average estimation error
--Cash, by hotel group (max is Marriott at +1.3%)
SELECT hotel_group,
--	COALESCE(cp_gap,0)+COALESCE(cn_gap,0) as gap,
	count(*) as count,
	sum(cash_est)/sum(cash)-1 as cash_err
FROM hotel_sample_est
WHERE cash_est IS NOT NULL and cash IS NOT NULL
GROUP BY 1;

-- Points, by hotel group (max is Marriott at -0.3%)
SELECT hotel_group,
	count(*) as count,
	sum(points_est)/sum(points)-1 as cash_err
FROM hotel_sample_est
WHERE points_est IS NOT NULL and points IS NOT NULL
GROUP BY 1;

-- Average cash error by gap size
SELECT 
	COALESCE(cp_gap,0)+COALESCE(cn_gap,0) as gap,
	count(*) as count,
	sum(cash_est)/sum(cash)-1 as cash_err
FROM hotel_sample_est
WHERE cash_est IS NOT NULL and cash IS NOT NULL
GROUP BY 1;

-- Average points error by gap size
SELECT 
	COALESCE(pp_gap,0)+COALESCE(pn_gap,0) as gap,
	count(*) as count,
	sum(points_est)/sum(points)-1 as points_err
FROM hotel_sample_est
WHERE points_est IS NOT NULL and points IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- QA for hotel_name_key data coverage after interpolation --
-- How many NULL values per hotel_name_key, after coalescing estimate?
SELECT hotel_group, hotel_name_key,
	sum(CASE WHEN cash_best IS NULL THEN 1 END) as cash_null,
	sum(CASE WHEN points_best IS NULL THEN 1 END) as points_null,
	sum(CASE WHEN cash_best IS NOT NULL AND points_best IS NOT NULL THEN 1 END) as both_valid,
	min(CASE WHEN cash_best IS NULL OR points_best IS NULL THEN date_booked END) as min_date_booked_null,
	max(CASE WHEN cash_best IS NULL OR points_best IS NULL THEN date_booked END) as max_date_booked_null,
	count(*) as count
FROM (
	SELECT hotel_group, hotel_name_key, date_stay, date_booked, booking_window, cash, points,
		COALESCE(cash, cash_est) as cash_best,
		COALESCE(points, points_est) as points_best
	FROM hotel_sample_est
) a
GROUP BY 1, 2
ORDER BY 5 DESC;

--- #### ANALYSIS #### ----

-- Value per 100 points by chain
SELECT hotel_group,
	sum(cash_value)/sum(points)*100.0 as value_100_pts
FROM hotel
WHERE cash_value is not null and points is not null
GROUP BY 1
ORDER BY 1;

-- Value per 100 points, by month scraped
SELECT hotel_group, date(date_trunc('month',date_booked)) as month_scraped,
	sum(cash_value)/sum(points)*100.0 as value_100_pts
FROM hotel
WHERE cash_value is not null and points is not null
GROUP BY 1,2
ORDER BY 1,2;

-- Value per 100 points, by month of booking
SELECT hotel_group, date(date_trunc('month',date_stay)) as month_booking,
	sum(cash_value)/sum(points)*100.0 as value_100_pts
FROM hotel
WHERE cash_value is not null and points is not null
GROUP BY 1,2
ORDER BY 1,2;

-- Value per 100 points, by price tier
SELECT hotel_group,
	CASE WHEN cash_value < 0.9 * avg_price THEN 'low_demand' WHEN cash_value >= 0.9 * avg_price AND cash_value <= 1.1 * avg_price THEN 'mid_demand' ELSE 'high_demand' END as demand_tier,
	sum(cash_value)/sum(points)*100.0 as value_100_pts
FROM hotel a
LEFT JOIN (
	SELECT hotel_group, hotel_name_key, city, avg(cash_value::NUMERIC) as avg_price
	FROM hotel
	GROUP BY 1,2,3
) b
USING (hotel_group, hotel_name_key, city)
WHERE cash_value is not null and points is not null
GROUP BY 1,2
ORDER BY 1,2;

-- Points value, by booking window
SELECT hotel_group,
	FLOOR(booking_window::NUMERIC/30) as months_in_advance,
	sum(cash_value)/sum(points)*100.0 as value_100_pts,
	count(*) as n
FROM hotel
WHERE cash_value is not null and points is not null
GROUP BY 1,2
ORDER BY 1,2;