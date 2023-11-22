/*The Operations team would like to know which countries we currently operate in and 
which country now has the most stores.*/

SELECT 
	DISTINCT(country_code),
	COUNT(country_code)
FROM dim_store_details 
GROUP BY country_code

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
LIMIT 10

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
LIMIT 6

/* output:
"08"	673295.679999988
"01"	668041.4499999896
"10"	657335.8399999888
"05"	650321.4299999895
"07"	645741.6999999885
"03"	645462.9999999898 */























