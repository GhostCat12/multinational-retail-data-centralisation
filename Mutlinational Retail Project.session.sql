
/*first commemt */
ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(19);
ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(12);
ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR(12);
ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT;

/*commemt */                          
ALTER TABLE dim_users ALTER COLUMN first_name TYPE VARCHAR(255);
ALTER TABLE dim_users ALTER COLUMN last_name TYPE VARCHAR(255);
ALTER TABLE dim_users ALTER COLUMN date_of_birth TYPE DATE;
ALTER TABLE dim_users ALTER COLUMN country_code TYPE VARCHAR(2);
ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE dim_users ALTER COLUMN join_date TYPE DATE;

/*commemt */   
ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;
ALTER TABLE dim_store_details ALTER COLUMN locality TYPE VARCHAR(255);
ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE VARCHAR(12);
ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;
ALTER TABLE dim_store_details ALTER COLUMN opening_date TYPE DATE;
ALTER TABLE dim_store_details ALTER COLUMN store_type TYPE VARCHAR(255);
ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE FLOAT USING longitude::double precision;
ALTER TABLE dim_store_details ALTER COLUMN country_code TYPE VARCHAR(2);
ALTER TABLE dim_store_details ALTER COLUMN continent TYPE VARCHAR(255);                             

/*# There is a row that represents the business's website change the location column values where they're null to N/A.*/
UPDATE dim_store_details SET locality = coalesce(locality, 'N/A');
UPDATE dim_store_details SET address = coalesce(address, 'N/A');

                              
/*commemt */  


ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);
UPDATE dim_products
SET weight_class =
CASE
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid-Sized' 
    WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END;           
                              
/*You will want to rename the removed column to still_available and changing data types*/  
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;
ALTER TABLE dim_products ALTER COLUMN product_price_£ TYPE FLOAT;
ALTER TABLE dim_products ALTER COLUMN weight_kg TYPE FLOAT;
ALTER TABLE dim_products ALTER COLUMN "EAN" TYPE VARCHAR(17);
ALTER TABLE dim_products ALTER COLUMN product_code TYPE VARCHAR(12);
ALTER TABLE dim_products ALTER COLUMN date_added TYPE DATE;
ALTER TABLE dim_products ALTER COLUMN uuid TYPE UUID USING uuid::uuid;

UPDATE dim_products
SET still_available = ( CASE WHEN still_available = 'still_available' 
                   THEN 1 else 0 
               END);
ALTER TABLE dim_products ALTER COLUMN still_available TYPE BOOL USING still_available::boolean;
ALTER TABLE dim_products ALTER COLUMN weight_class TYPE VARCHAR(14);

/*commemt */
ALTER TABLE dim_date_times ALTER COLUMN month TYPE VARCHAR(2);
ALTER TABLE dim_date_times ALTER COLUMN year TYPE VARCHAR(4);
ALTER TABLE dim_date_times ALTER COLUMN day TYPE VARCHAR(2);
ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE VARCHAR(10);
ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

 /*commemt */                                                           
ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(19);
ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE VARCHAR(5);
ALTER TABLE dim_card_details ALTER COLUMN date_payment_confirmed TYPE DATE;


