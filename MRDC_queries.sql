/*The Operations team would like to know which countries we currently operate in and 
which country now has the most stores.*/

SELECT 
	DISTINCT(country_code),
	COUNT(country_code) as total_number_of_stores
FROM dim_store_details 
GROUP BY country_code
ORDER BY total_number_of_stores DESC;




/* The business stakeholders would like to know which locations currently have the most stores.
They would like to close some stores before opening more in other locations.
Find out which locations have the most stores currently*/

SELECT 
	DISTINCT(locality),
	COUNT(locality) AS total_number_stores
FROM dim_store_details 
GROUP BY locality
ORDER BY COUNT(locality) DESC
LIMIT 10;



/* Query the database to find out which months have produced the most sales.*/

SELECT 
	DISTINCT("month"),
	SUM("product_price_£" * "product_quantity") AS total_sales
FROM dim_date_times
    FULL OUTER JOIN orders_table 
		ON dim_date_times.date_uuid = orders_table.date_uuid
	FULL OUTER JOIN dim_products
		ON orders_table.product_code = dim_products.product_code
GROUP BY "month"
ORDER BY SUM("product_price_£" * "product_quantity") DESC
LIMIT 6;



/* The company is looking to increase its online sales. They want to know how many sales are happening online vs offline. 
Calculate how many products were sold and the amount of sales made for online and offline purchases. */

SELECT 
	CASE 
		WHEN store_code = 'WEB-1388012W' THEN 'Web'
		ELSE 'Offline'
	END AS "location",
	SUM(product_quantity) AS product_quantity_count,
	COUNT(product_quantity*product_price_£) AS number_of_sales
FROM orders_table
FULL OUTER JOIN dim_products
	ON orders_table.product_code = dim_products.product_code
GROUP BY "store_code" = 'WEB-1388012W';



/* The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
Find out the total and percentage of sales coming from each of the different store types. */

WITH store_sales(store_type,total_sales)
AS
(
	SELECT 
		store_type,
		SUM(product_quantity * product_price_£) AS total_sales
	FROM orders_table
		FULL OUTER JOIN dim_products
		ON orders_table.product_code = dim_products.product_code
		FULL OUTER JOIN dim_store_details
		ON dim_store_details.store_code = orders_table.store_code
	GROUP BY store_type
)
SELECT 
	store_type,
	total_sales,
	total_sales * 100 / (SELECT SUM(total_sales) FROM store_sales) AS "percentage_total(%)"
FROM store_sales
ORDER BY "percentage_total(%)" DESC;



/* The company stakeholders want assurances that the company has been doing well recently.
Find which months in which years have had the most sales historically. */

SELECT 
	SUM(product_price_£ * product_quantity) AS total_sales,
	"year",
	"month"
FROM orders_table
FULL OUTER JOIN dim_date_times
	ON orders_table.date_uuid = dim_date_times.date_uuid
FULL OUTER JOIN dim_products
 ON orders_table.product_code = dim_products.product_code
GROUP BY "year", "month"
ORDER BY total_sales DESC
LIMIT 10;



/*The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.
The query should return the values:*/

SELECT 
	SUM(staff_numbers) AS total_staff_numbers,
	country_code 
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;



/*The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany*/

SELECT 
	SUM(product_price_£ * product_quantity) AS total_sales,
	store_type,
	country_code
FROM orders_table
FULL OUTER JOIN dim_store_details
	ON orders_table.store_code = dim_store_details.store_code
FULL OUTER JOIN dim_products
	ON orders_table.product_code = dim_products.product_code
WHERE "country_code" = 'DE'
GROUP by store_type, country_code
ORDER BY total_sales DESC;



/*Sales would like the get an accurate metric for how quickly the company is making sales.
Determine the average time taken between each sale grouped by year*/

with combine_datetime("year","month","day","timestamp",full_date_time)     /*combines year, month, day, timestamp columns into a datetime format*/
as (
	SELECT 
	"year",
	"month",
	"day",
	"timestamp",
	TO_TIMESTAMP(("year" || '-' || "month" || '-' || "day" || ' ' || "timestamp"), 'YYYY-MM-DD HH24:MI:SS.MS')::timestamp without time zone AS full_date_time
	FROM dim_date_times
	),
	
next_time("year", full_date_time, next_timestamp)     /* creates column holding datetime of next row */
as (
	SELECT
		"year",
		full_date_time,
	LEAD(full_date_time, 1 ) OVER (ORDER BY full_date_time) AS next_timestamp
	FROM combine_datetime
),

avg_time_difference("year", date_difference)    /* find time difference between initial datetime column and next row column */
as (

	SELECT 
		"year",
		AVG((next_timestamp - full_date_time)) as date_difference	
	FROM next_time 
	GROUP BY "year"
)
SELECT     /* separates date and time into required output format  */
"year",
	CONCAT('hours: ' , CAST(DATE_PART('hour' , "date_difference") as VARCHAR),
		   ',  minutes: ' , CAST(DATE_PART('minute' , "date_difference") as VARCHAR),
		   ',  seconds: ' , CAST(ROUND(EXTRACT('second' FROM "date_difference"), 3) as VARCHAR)
		  ) as actual_time_taken
FROM avg_time_difference
ORDER BY date_difference DESC
LIMIT 5;
