-- award queries
SELECT hotel_group, hotel_name_key,
	count(*),
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 2 THEN 1 END) as Tuesdays,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 1 THEN 1 END)*1.0/count(*) as pct_mon,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 2 THEN 1 END)*1.0/count(*) as pct_tues,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 3 THEN 1 END)*1.0/count(*) as pct_wed,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 4 THEN 1 END)*1.0/count(*) as pct_thu,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 5 THEN 1 END)*1.0/count(*) as pct_fri,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 6 THEN 1 END)*1.0/count(*) as pct_sat,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 0 THEN 1 END)*1.0/count(*) as pct_sun,
	min(created_at) as min_date,
	max(created_at) as max_date
FROM hotel_award_test
GROUP BY 1,2
ORDER BY 1,3;


SELECT hotel_group, hotel_name_key,
	count(*),
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 2 THEN 1 END) as Tuesdays,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 1 THEN 1 END)*1.0/count(*) as pct_mon,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 2 THEN 1 END)*1.0/count(*) as pct_tues,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 3 THEN 1 END)*1.0/count(*) as pct_wed,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 4 THEN 1 END)*1.0/count(*) as pct_thu,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 5 THEN 1 END)*1.0/count(*) as pct_fri,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 6 THEN 1 END)*1.0/count(*) as pct_sat,
	SUM(CASE WHEN EXTRACT(DOW FROM created_at) = 0 THEN 1 END)*1.0/count(*) as pct_sun,
	min(created_at) as min_date,
	max(created_at) as max_date
FROM hotel_cash
GROUP BY 1,2
ORDER BY 1,3;

-- hotel_group summary stats
SELECT hotel_group, count(*),
	min(created_at) as min_dt,
	max(created_at) as max_dt
FROM hotel_cash
GROUP BY 1
ORDER BY 1;

-- hotel_name_key summary stats
SELECT hotel_group, hotel_name_key,	count(*),
	min(created_at) as min_dt,
	max(created_at) as max_dt
FROM hotel_cash
GROUP BY 1,2
ORDER BY 1,3;

-- Dupes check?
SELECT _id, count(*)
FROM hotel_cash
GROUP BY 1
HAVING count(*) > 1
ORDER BY 2 desc;

-- Data sample output query
SELECT *
FROM hotel_cash
WHERE hotel_name_key = 'hampton-inn-manhattan-times-square-central'
ORDER BY created_at, date_booking;


SELECT date_booking, AVG(cash_value) as cash_value, count(*)
FROM hotel_cash
WHERE hotel_name_key = 'hampton-inn-manhattan-times-square-central'
GROUP BY 1
ORDER BY date_booking;

-- Average price of bookings made 30-60 days out, by hotel_name_key and mon-thurs vs fri-sun.
WITH table_in as (
	SELECT hotel_group, hotel_name_key, date_booking,
		ROUND(AVG(cash_value)::numeric,2) as cash_value,
		ROUND(AVG(CASE WHEN EXTRACT(DOW FROM created_at) BETWEEN 1 AND 4 THEN cash_value END)::numeric,2) as m_th_cash,
		ROUND(AVG(CASE WHEN EXTRACT(DOW FROM created_at) NOT BETWEEN 1 AND 4 THEN cash_value END)::numeric,2) as f_su_cash,
		count(*) as count
	FROM hotel_cash
	WHERE FLOOR(EXTRACT(EPOCH FROM (date_booking - created_at))/86400) BETWEEN 30 and 60 -- Bookings made 30-60 days in advance
		AND cash_value > 0
	GROUP BY 1,2,3
	ORDER BY date_booking
)
SELECT hotel_group, hotel_name_key,
	ROUND(avg(m_th_cash),2) as m_th_cash,
	ROUND(avg(f_su_cash),2) as f_su_cash,
	sum(count) as count
FROM table_in
WHERE count > 10
GROUP BY 1,2
ORDER BY 1,2;

-- Daily, weekly observations by hotel_name_key
SELECT hotel_group, hotel_name_key,
	DATE_TRUNC('WEEK', created_at) as created_week,
	count(*) as count
FROM hotel_cash
GROUP BY 1,2,3
ORDER BY 1,2,3;

--- Spot checks of a hotel with many (482k) observations for a week of date_booking
SELECT *
FROM hotel_cash
WHERE hotel_name_key = 'hampton-inn-manhattan-times-square-central'
ORDER BY date_booking, created_at;

-- only 75-90% of rows are duplicated - same collected date, same booking date, 
WITH processed_data as (
	SELECT *, row_number() OVER (PARTITION BY hotel_group, hotel_name_key, award_category, date_booking, date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_cash
	WHERE currency = 'USD' and cash_value > 1
)
SELECT hotel_group, sum(case when rn = 1 then 1 end) as clean_count, count(*) as total_count
FROM processed_data
GROUP BY 1
ORDER BY 1;

-- By hotel_name_key
WITH processed_data as (
	SELECT *, row_number() OVER (PARTITION BY hotel_group, hotel_name_key, award_category, date_booking, date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_cash
	WHERE currency = 'USD' and cash_value > 1
)
SELECT hotel_group, hotel_name_key,
	sum(case when rn = 1 then 1 end) as clean_count,
	count(*) as total_count,
	sum(case when rn = 1 then 1.0 end)/count(*) as pct_needed
FROM processed_data
GROUP BY 1,2
ORDER BY 1;

-- spot check dupe rows
WITH processed_data as (
	SELECT *, row_number() OVER (PARTITION BY hotel_group, hotel_name_key, award_category, date_booking, date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_cash
	WHERE currency = 'USD' and cash_value > 1
		AND hotel_name_key = 'distrikt-hotel-new-york-city-tapestry-collection-by-hilton'
)
SELECT *
FROM processed_data
WHERE rn > 1
	AND date(date_booking) = '2023-08-01'
ORDER BY hotel_group, hotel_name_key, date_booking, date(created_at);

-- Create clean table
DROP TABLE IF EXISTS hotel_cash_clean;
CREATE TABLE hotel_cash_clean AS

WITH first_table as (
	SELECT *, row_number() OVER (PARTITION BY hotel_group, hotel_name_key, award_category, date_booking, date(created_at) ORDER BY created_at desc) as rn
	FROM hotel_cash
	WHERE currency = 'USD' and cash_value > 1
)
SELECT hotel_group, hotel_name_key, city, date_booking, cash_value, currency, created_at, award_category, _id
FROM first_table
WHERE rn = 1;

-- QA marriott
SELECT date(created_at) as created_date, count(*)
FROM hotel_cash_clean
WHERE hotel_group = 'marriott'
GROUP BY 1
ORDER BY 1;

SELECT date_trunc('week',created_at) as created_date, count(*)
FROM hotel_cash_clean
WHERE hotel_group = 'marriott'
GROUP BY 1
ORDER BY 1;