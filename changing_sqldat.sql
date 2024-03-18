-- CASTING orders_table TABLE COLUMNS TO CORRECT DATA TYPES-------------------
SELECT * FROM orders_table;

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

SELECT max(length(CAST(card_number as text))) as card_number_lngth FROM orders_table;--19
SELECT max(length(store_code)) as store_code_lngth FROM orders_table; --12
SELECT max(length(product_code)) as product_code_lngth FROM orders_table; --11

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE varchar(19);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE varchar(12);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE varchar(11);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;


-- -- CASTING dim_users TABLE COLUMNS TO CORRECT DATA TYPES-------------------


ALTER TABLE dim_users
ALTER COLUMN first_name TYPE varchar(255);

ALTER TABLE dim_users
ALTER COLUMN last_name TYPE varchar(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE date;

SELECT max(length(country_code)) as country_code_lngth FROM dim_store_details; --2
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE varchar(2);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE;

-- CASTING dim_store_details TABLE COLUMNS TO CORRECT DATA TYPES-------------------    


ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE varchar(255);

SELECT max(length(store_code)) as store_code_lngth FROM dim_store_details; --11
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(11);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT;

SELECT max(length(country_code)) as country_code_lngth FROM dim_store_details; --2
ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);


-- CONSTRUCTING & ADDING NEW WEIGHT COLUMN TO dim_products TABLE-------------------

UPDATE dim_products
SET product_price = LTRIM(product_price,'£');
ALTER TABLE dim_products

ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision;

ALTER TABLE dim_products
ADD COLUMN weight_class TEXT;

UPDATE dim_products
SET weight_class = 'Light'
WHERE weight<2;

UPDATE dim_products
SET weight_class = 'Mid_Sized'
WHERE weight>=2 AND weight<40;

UPDATE dim_products
SET weight_class = 'Heavy'
WHERE weight>=40 AND weight<140;

UPDATE dim_products
SET weight_class = 'Truck_Required'
WHERE weight>=140;


-- CASTING dim_products TABLE COLUMNS TO CORRECT DATA TYPES-------------------


ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT;

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT;

-- ALTER TABLE dim_products
-- ALTER COLUMN "EAN" TYPE TEXT;
-- SELECT max(len("EAN")) as EAN_lngth FROM dim_products; --17
ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);

-- SELECT max(length(product_code)) as product_code_lngth FROM dim_products; --11
ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::uuid;

ALTER TABLE dim_products 
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = 'yes'
WHERE still_available = 'Still_avaliable';
SELECT still_available FROM dim_products;

UPDATE dim_products
SET still_available = 'no'
WHERE still_available = 'Removed';
SELECT still_available FROM dim_products;

ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL USING still_available::boolean;

-- SELECT MAX(LENGTH(weight_class)) FROM dim_products;--14
ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(14);


-- CASTING dim_date_times TABLE COLUMNS TO CORRECT DATA TYPES-------------------


SELECT MAX(LENGTH(month)) FROM dim_date_times; --2
SELECT MAX(LENGTH(year)) FROM dim_date_times; --4
SELECT MAX(LENGTH(day)) FROM dim_date_times; --2
SELECT MAX(LENGTH(time_period)) FROM dim_date_times; --10

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2);
ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4);
ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2);
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;


-- CASTING dim_card_details TABLE COLUMNS TO CORRECT DATA TYPES-------------------


ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE TEXT;
ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE TEXT;
ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE TEXT;

SELECT MAX(LENGTH(card_number)) FROM dim_card_details; --22
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details; --19
SELECT MAX(LENGTH(date_payment_confirmed)) FROM dim_card_details; --19

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22);
ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(19);
ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;


-- ADDING RELEVANT PRIMARY KEYS-------------------------------------------------------


SELECT * FROM dim_card_details;
ALTER TABLE dim_card_details 
ADD PRIMARY KEY (card_number);

SELECT * FROM dim_date_times;
ALTER TABLE dim_date_times 
ADD PRIMARY KEY (date_uuid);

SELECT * FROM dim_products;
ALTER TABLE dim_products 
ADD PRIMARY KEY (product_code);

SELECT * FROM dim_store_details;
ALTER TABLE dim_store_details 
ADD PRIMARY KEY (store_code);

SELECT * FROM dim_users;
ALTER TABLE dim_users 
ADD PRIMARY KEY (user_uuid);


-- ADDING RELEVANT FOREIGN KEYS ------------------------------------------------------


ALTER TABLE orders_table 
ADD CONSTRAINT card_number_fkey
FOREIGN KEY (card_number) 
REFERENCES dim_card_details (card_number);

SELECT * FROM dim_date_times;
ALTER TABLE orders_table 
ADD CONSTRAINT date_uuid_fkey
FOREIGN KEY (date_uuid) 
REFERENCES dim_date_times (date_uuid);

SELECT * FROM dim_products;
ALTER TABLE orders_table 
ADD CONSTRAINT product_code_fkey
FOREIGN KEY (product_code) 
REFERENCES dim_products (product_code);

-- SELECT store_code FROM dim_store_details
-- WHERE store_code='WEB-1388012W';
-- SELECT length(store_code) FROM orders_table
-- WHERE store_code='WEB-1388012W';

ALTER TABLE orders_table 
ADD CONSTRAINT store_code_fkey
FOREIGN KEY (store_code) 
REFERENCES dim_store_details (store_code);

SELECT * FROM dim_users;
ALTER TABLE orders_table 
ADD CONSTRAINT user_uuid_fkey
FOREIGN KEY (user_uuid) 
REFERENCES dim_users (user_uuid);