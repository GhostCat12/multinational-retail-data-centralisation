
# Multinational Retail Data Centralisation


## Table of Contents:
#### 1. Project description and aims
#### 2. What I've learnt
#### 3. Installation instructions
#### 4. Usage instructions
#### 5. File structure of the project
#### 6. Up-to-date business metrics 
#### 7. License information 



## Project description and aims: 
### Case scenario:

"You work for a multinational company that sells various goods across the globe.
Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team."



### The aim: 
Making sales data accessible by gathering data from different resources. Cleaning all data and developing a star based schema database in which important metrics can be queried on from one centralised location for a multinational company that sells various goods across the globe. 



## Major milestones:

### The project is split into 4 milestones:
1.  Setting up a conda environment for data replicability 

2.  The use of Python classes and methods for: 
    - Extracting data from various data sources including PDFs, CSVs, Relational Database Tables, S3 Buckets and Web APIs. Learning the use of Threads to efficiently extract a large number of WEB APIs in parallel to one another. 
    - Cleaning the data by mainly utilising Pandas and Numpy. 
    - Uploading tables to a locally created database using SQLAlchemy engine.   

3. Creating a star-based database schema via SQL CRUD operations. Changing column data types and applying primary and foreign key constraints.  

4. Querying the data on pgAdmin4 via subqueries, joining tables and CTEs.  


## Installation instructions:
    conda env create -f mrdc_env.yaml
    conda activate mrdc_env 


## Usage instructions:
    python3 main.py


## File structure of the project:

#### 1. main.py  
Contains code to run different methods from different classes within different files in one location 

#### 2. data_cleaning.py  
Contains the 'DataCleaning' class holding the following methods to clean the data:
- clean_user_data()
- clean_card_data()
- clean_store_data()
- convert_product_weights()
- clean_products_data() 
- clean_orders_data() 
- clean_date_details_data()     

#### 3. data_extraction.py
Contains the 'DataExtractor' class holding the following methods to extract the data:
- read_rds_table()
- retrieve_pdf_data()
- list_number_of_stores()
- fetch_url()
- retrieve_stores_data()
- extract_from_s3()

#### 4. database_utils.py  
Contains the 'DatabaseConnector' class holding the following methods to connect the data:
- read_db_creds()
- init_db_engine()
- list_db_tables()
- init_local_db_engine()
- upload_to_db()

#### 5. MRDC_alter_table_queries.sql 
Contains SQL CRUD operation queries for column alterations, creating primary and foreign key constraints for all tables.   


#### 6. MRDC_queries.sql
Contains SQL queries run for insight into business metrics questions (outputs covered in the "Up to date business metrics" section).  


## Up-to-date business metrics 



#### How many stores does the business have and in which countries? 


| country  | total_no_stores |
|----------|-----------------|
| GB       |             266 |
| DE       |             141 |
| US       |              34 |


#### Which locations currently have the most stores? 

| locality      | total_no_stores|
|---------------|-----|
|Chapletown     |	14|
|Belper         |	13|
|Bushey         |	12|
|Exeter         |	11|
|Arbroath       |	10|
|High Wycombe   |	10|
|Rutherglen     |	10|
|Aberdeen       |	9 |
|Lancing        |	9 |
|Landshut       |	9 |



#### Which months produced the largest amount of sales?


|month|total_sales|
|-----|-----------|
|08|	673295.68|
|01|	668041.45|
|10|	657335.84|
|05|	650321.43|
|07|	645741.70|
|03|	645463.00|
 

#### How many sales are coming from online? 
|location | products_quantity_count | number_of_sales|
|---------|-------------------------|----------------|
|Offline  |374047                   |	93166        |
|Web	  |107739                   |	26957        |


#### What percentage of sales come through each type of store? 

|store_type|	total_sales|	percentage_total(%)|
|----------| -------------------|---------------------|
|Local	    |3440896.52	|44.56  |
|Web Portal|	1726547.05|	22.36|
|Super Store|	1224293.65|	15.86|
|Mall Kiosk|	698791.61|	9.05|
|Outlet|	631804.81|	8.18|
#### Which month in each year produced the highest cost of sales? 

|total_sales|	year	|month|
|------------|---------|--------|
|27936.77	     |1994|	    03|
|27356.14   	|2019|	    01|
|27091.67        |2009|	    08|
|26679.98   	|1997|	    11|
|26310.97	    |2018|	    12|
|26277.72   	|2019|	    08|
|26236.67	    |2017|	    09|
|25798.12	    |2010|	    05|
|25648.29	    |1996|	    08|
|25614.54	    |2000|	    01|

#### What is our staff headcount?



| total_staff_numbers | country_code |
|---------------------|--------------|
|               13307 | GB        |
|                6123 | DE       |
|                1384 | US        |


#### Which German store type is selling the most?
| total_sales|   store_type| 	country_code| 
| --------------| --------| -------| 
| 1109909.5899999617| 	 Local| 	 DE| 
| 384625.02999999834| 	Super Store| 	DE| 
| 247634.20000000042| 	Mall Kiosk| 	DE| 
| 198373.57000000039| 	Outlet| 	DE| 

#### How quickly is the company making sales?

|year|	actual_time_taken|
|----|--------------------|
|2013|	hours: 2,  minutes: 17,  seconds: 15.655|
|1993|	hours: 2,  minutes: 15,  seconds: 40.130|
|2002|	hours: 2,  minutes: 13,  seconds: 49.478|
|2008|	hours: 2,  minutes: 13,  seconds: 3.532|
|2022|	hours: 2,  minutes: 13,  seconds: 2.004|



## License information
GNU General Public License (GPL) v3.0
