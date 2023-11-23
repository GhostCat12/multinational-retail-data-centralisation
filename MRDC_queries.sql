/*The Operations team would like to know which countries we currently operate in and 
which country now has the most stores.*/

SELECT 
	DISTINCT(country_code),
	COUNT(country_code)
FROM dim_store_details 
GROUP BY country_code;

/* output: 
"DE"	141
"GB"	266
"US"	34  */




/* The business stakeholders would like to know which locations currently have the most stores.
They would like to close some stores before opening more in other locations.
Find out which locations have the most stores currently*/

SELECT 
	DISTINCT(locality),
	COUNT(locality)
FROM dim_store_details 
GROUP BY locality
ORDER BY COUNT(locality) DESC
LIMIT 10;

/* output:
"Chapletown"	14
"Belper"	    13
"Bushey"	    12
"Exeter"	    11
"Arbroath"	    10
"High Wycombe"	10
"Rutherglen"	10
"Aberdeen"	     9
"Lancing"	     9
"Landshut"	     9 */




/* Query the database to find out which months have produced the most sales.*/

SELECT 
	DISTINCT("month"),
	SUM("product_price_£" * "product_quantity")
FROM dim_date_times
    FULL OUTER JOIN orders_table 
		ON dim_date_times.date_uuid = orders_table.date_uuid
	FULL OUTER JOIN dim_products
		ON orders_table.product_code = dim_products.product_code
GROUP BY "month"
ORDER BY SUM("product_price_£" * "product_quantity") DESC
LIMIT 6;

/* output:
"08"	673295.679999988
"01"	668041.4499999896
"10"	657335.8399999888
"05"	650321.4299999895
"07"	645741.6999999885
"03"	645462.9999999898 */


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

/* Output 
Location   product_quantity_count 	number_of_sales
"Offline"  374047	                93166
"Web"	   107739	                26957
*/


/* The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
Find out the total and percentage of sales coming from each of the different store types. */

with t(store_type,total_sales)
as
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
	total_sales * 100 / (SELECT SUM(total_sales) FROM t) as percentage
FROM t;

/*
store_type   	   total_sales  	  percentage
"Super Store"	1224293.6499999538	15.853933630352115
"Web Portal"	1726547.0499999092	22.357840653980215
"Local"	3440896.5200001546	44.55772931354668
"Outlet"	631804.8099999899	8.18152697686331
"Mall Kiosk"	698791.6099999876	9.048969425257674
*/


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
ORDER BY total_sales DESC;

/* 
total_sales              year   month
27936.769999999993	    "1994"	"03"
27356.14	            "2019"	"01"
27091.66999999999	    "2009"	"08"
26679.979999999992	    "1997"	"11"
26310.969999999998	    "2018"	"12"
26277.719999999994	    "2019"	"08"
26236.670000000006	    "2017"	"09"
25798.119999999995	    "2010"	"05"
25648.28999999999	    "1996"	"08"
25614.539999999983	    "2000"	"01"
*/

/*The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.
The query should return the values:*/


SELECT
SUM(staff_numbers) AS total_staff_numbers,
dim_store_details.country_code
FROM orders_table
FULL OUTER JOIN dim_store_details
	ON orders_table.store_code = dim_store_details.store_Code                          ### 	QUESTION THIS 	###
GROUP BY dim_store_details.country_code;

/*
total_staff_numbers  Country
298156	                "US"
11506604	            "GB"
1307021	                "DE"
*/

/*The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany*/

SELECT 
SUM(product_price_£ * product_quantity) AS total_sales,
store_type,
country_code
FROM orders_table
FULL OUTER JOIN dim_store_details
	ON orders_table.store_code = dim_store_details.store_code
FULL OUTER JOIN dim_products
	ON orders_table.product_Code = dim_products.product_Code
WHERE "country_code" = 'DE'
GROUP by store_type, country_code;

/*
total_sales         store_type      country_code
384625.02999999834	"Super Store"	"DE"
247634.20000000042	"Mall Kiosk"	"DE"
1109909.5899999617	"Local"	        "DE"
198373.57000000039	"Outlet"	    "DE"
*/


/*Sales would like the get an accurate metric for how quickly the company is making sales.
Determine the average time taken between each sale grouped by year*/

with combine_datetime("year","month","day","timestamp",full_date_time)
as (
	SELECT 
	"year",
	"month",
	"day",
	"timestamp",
	TO_TIMESTAMP(("year" || '-' || "month" || '-' || "day" || ' ' || "timestamp"), 'YYYY-MM-DD HH24:MI:SS.MS')::timestamp without time zone AS full_date_time
	FROM dim_date_times
	),
	
next_time("year", full_date_time, next_timestamp)
as (
	SELECT
		"year",
		full_date_time,
	LEAD(full_date_time, 1 ) OVER (ORDER BY full_date_time) AS next_timestamp
	FROM combine_datetime
),

avg_time_difference("year", date_difference)
as (

	SELECT 
		"year",
		AVG((next_timestamp - full_date_time)) as date_difference	
	FROM next_time 
	GROUP BY "year"
)
SELECT 
"year",
	CONCAT('hours: ' , CAST(DATE_PART('hour' , "date_difference") as VARCHAR),
		   ',  minutes: ' , CAST(DATE_PART('minute' , "date_difference") as VARCHAR),
		   ',  seconds: ' , CAST(ROUND(extract('second' FROM "date_difference"), 3) as VARCHAR)
		  )
FROM avg_time_difference
ORDER BY date_difference DESC
LIMIT 5;

/*  output
"2013"	"hours: 2,  minutes: 17,  seconds: 15.655"
"1993"	"hours: 2,  minutes: 15,  seconds: 40.130"
"2002"	"hours: 2,  minutes: 13,  seconds: 49.478"
"2008"	"hours: 2,  minutes: 13,  seconds: 3.532"
"2022"	"hours: 2,  minutes: 13,  seconds: 2.004" */