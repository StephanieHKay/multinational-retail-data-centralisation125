-- (Milestone 3, Task 1) undersand the current orders_table data types
SELECT *
FROM information_schema.columns
WHERE  table_name = 'orders_table';

--(Milestone 3, Task 1) change card_number column to text (from int) so it can be changed to varchar
ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE TEXT;
	
--(Milestone 3, Task 1) find longest value length to use in the below change to data type
SELECT MAX(LENGTH(store_code)) AS store_code_max_length,		
		MAX(LENGTH(product_code)) AS product_code_max_length,
		MAX(length(card_number)) AS card_number_max_length
FROM orders_table;	
	
-- (Milestone 3, Task 1)apply the correct data types to the columns of the orders_table	
ALTER TABLE orders_table
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN product_quantity TYPE smallint,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11)
	;

-- (Milestone 3, Task 2) undersand the current dim_users data types
SELECT *
FROM information_schema.columns
WHERE  table_name = 'dim_users';

SELECT *
FROM dim_users;

--  (Milestone 3, Task 2) find longest value length to use in the below change to data type
SELECT MAX(LENGTH(country_code)) AS country_code_max_length		
FROM dim_users;	

-- (Milestone 3, Task 2) apply the correct data types to the columns of the dim_users	
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),	
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE USING join_date::date
	;


-- (Milestone 3, Task 3) apply correct data types to columns in dim_store_details
SELECT *
FROM information_schema.columns
WHERE  table_name = 'dim_store_details';


-- (Milestone 3, Task 3) find max lengths of store_code and country_code
SELECT MAX(LENGTH(country_code)) AS country_code_max_length_store,
		MAX(LENGTH(store_code)) AS store_code_max_length_store
FROM dim_store_details;	


-- (Milestone 3, Task 3) apply the correct data types to the columns of the dim_store_details	
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE,	
	ALTER COLUMN latitude TYPE FLOAT,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255),
	ALTER COLUMN store_type TYPE VARCHAR(255);

-- (Milestone 3, Task 3) find row that represents the business website in dim_store_details
SELECT *
FROM dim_store_details
WHERE store_type = 'Web Portal'
--WHERE store_type = 'Web Portal', WHERE locality = 'N/A', WHERE latitude = ?'NaN'
--WHERE address = 'N/A'
;





-- (Milestone 3, Task 4) £ already removed from price with python cleaning
SELECT *
FROM dim_products;

-- (Milestone 3, Task 4) create new weight_class column 

ALTER TABLE dim_products
	ADD COLUMN weight_class varchar(15);
	
-- (Milestone 3, Task 4) categorise weights in new weight_class column
UPDATE dim_products
SET weight_class = 
(CASE
WHEN weight < 2 THEN 'Light'
WHEN weight >= 2  AND weight < 40 THEN 'Mid_sized'
WHEN weight >= 40  AND weight < 140 THEN 'Heavy'
ELSE 'Truck_Required'
END) ;

-- (Milestone 3, Task 4) rename removed column to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- (Milestone 3, Task 4) change still available column to true false boolean

ALTER TABLE dim_products
ALTER still_available TYPE boolean
USING CASE still_available 
WHEN 'Still_avaliable' THEN true
WHEN 'Removed' THEN false
END;


-- (Milestone 3, Task 5) find EAN, product_code, weight_class lengths for below datatype change
SELECT MAX(LENGTH("EAN")) AS EAN_max_length, --17
		MAX(LENGTH(product_code)) AS product_code_max_length, --11
		MAX(LENGTH(weight_class)) AS weight_class_max_length -- 14		
FROM dim_products;	

-- (Milestone 3, Task 5) change dim_products data type
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT,
	ALTER COLUMN weight TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),	
	ALTER COLUMN date_added TYPE DATE USING date_added::date,	
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN still_available TYPE BOOL USING still_available::boolean,	
	ALTER COLUMN weight_class TYPE VARCHAR(14);



-- (Milestone 3, Task 6) update dim_date_times table with the correct types
SELECT *
FROM dim_date_times;

-- (Milestone 3, Task 6) find legnths of column variables to change
SELECT 
		MAX(LENGTH(time_period)) AS time_period_max_length -- 10		
FROM dim_date_times;	

-- (Milestone 3, Task 6) alter column types

ALTER TABLE dim_date_times	
	ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),	
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),	
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
	


-- (Milestone 3, Task 7) update dim_card_details
--current table 
SELECT *
FROM dim_card_details;

--  (Milestone 3, Task 7) column lengths
SELECT MAX(LENGTH(card_number)) AS card_number_length, --19
		MAX(LENGTH(expiry_date)) AS expiry_date_max_length --5	
FROM dim_card_details;	

-- (Milestone 3, Task 7) update column types
ALTER TABLE dim_card_details
	ALTER COLUMN expiry_date TYPE VARCHAR (5),
	ALTER COLUMN card_number TYPE VARCHAR(19),	
	ALTER COLUMN date_payment_confirmed TYPE DATE;


-- (Milestone 3, Task 8 and 9) find and create primary and foreign keys for each table

SELECT *
FROM orders_table;

--
SELECT *
FROM dim_card_details; --card_number is primary key

ALTER TABLE dim_card_details  --pk added to dim
ADD PRIMARY KEY (card_number);

ALTER TABLE orders_table--fk added to orders_table 
	ADD CONSTRAINT fk_orders_dim_card_details
	FOREIGN KEY (card_number)
	REFERENCES dim_card_details (card_number);
--
SELECT *
FROM dim_date_times; --date_uuid is primary key

ALTER TABLE dim_date_times--pk added to dim
ADD PRIMARY KEY (date_uuid);

ALTER TABLE orders_table --fk added to orders_table 
	ADD CONSTRAINT fk_orders_dim_date_times
	FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times (date_uuid);
--
SELECT *
FROM dim_products; --product_code is primary key

ALTER TABLE dim_products --pk added to dim
ADD PRIMARY KEY (product_code);

ALTER TABLE orders_table--fk added to orders_table 
	ADD CONSTRAINT fk_orders_dim_products
	FOREIGN KEY (product_code)
	REFERENCES dim_products (product_code);
--
SELECT *
FROM dim_store_details; --store_code is primary key

ALTER TABLE dim_store_details--pk added to dim
ADD PRIMARY KEY (store_code);

ALTER TABLE orders_table --fk added to orders_table 
	ADD CONSTRAINT fk_orders_dim_store_details
	FOREIGN KEY (store_code)
	REFERENCES dim_store_details (store_code);
--
SELECT *
FROM dim_users; -- user_uuid is primary key

ALTER TABLE dim_users--pk added to dim
ADD PRIMARY KEY (user_uuid);

ALTER TABLE orders_table --fk added to orders_table 
	ADD CONSTRAINT fk_orders_dim_users
	FOREIGN KEY (user_uuid)
	REFERENCES dim_users (user_uuid);





-- (Milestone 4, Task 1) find total_no_stores per country

SELECT country_code AS country,
		COUNT(country_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;


-- (Milestone 4, Task 2) find which localities have the most store
SELECT locality,
		COUNT(locality) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- drop index colum from tables
ALTER TABLE orders_table
DROP index;

ALTER TABLE dim_date_times
DROP index;

ALTER TABLE dim_card_details
DROP index;

ALTER TABLE dim_products
DROP index;

ALTER TABLE dim_store_details
DROP index;

--
--

-- (Milestone 4, Task 3) find which month produced the highest amount of sales
--dim_date_times has month of sale and date_uuid
--dim_product has the product price and product_code
--orders_table has the date_uuid and product code and product quantity
WITH sales_date_price AS (SELECT orders_table.date_uuid,
		orders_table.product_code,
		orders_table.product_quantity,
		dim_date_times.month,
		dim_products.product_price,
		(orders_table.product_quantity * dim_products.product_price) AS sum_of_product_sold
FROM orders_table
INNER JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code)

SELECT ROUND(SUM(sum_of_product_sold)::numeric,2) AS total_sales,
		month
FROM sales_date_price
GROUP BY month
ORDER BY total_sales DESC;


--(Milestone 4, Task 4) determine how many sales come from offline vs online (using dim_store_details and orders_table tables)

WITH sales_quantity_locality AS (SELECT orders_table.store_code,
		orders_table.product_quantity,
		dim_store_details.store_type, 
		CASE
	WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
	ELSE 
		'Offline'
	END AS location
FROM orders_table
INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code)

SELECT COUNT(product_quantity) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count,		
		location
FROM sales_quantity_locality
GROUP BY location
ORDER BY number_of_sales ASC;
		

--(Milestone 4, Task 5) percentage of sales from each store

WITH revenue_table AS (SELECT dim_store_details.store_type,
					   		ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric,2) AS sale_amount	
			FROM orders_table
		INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
		INNER JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
		GROUP BY dim_store_details.store_type),

overall_sales AS (SELECT SUM(dim_products.product_price * orders_table.product_quantity) AS percent_denom_sales
				FROM orders_table
				INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code)

SELECT r.store_type,
		r.sale_amount as total_sales,			
		ROUND((r.sale_amount / o.percent_denom_sales * 100 )::numeric, 2) AS "percentage_total(%)"
FROM revenue_table AS r, overall_sales as o
ORDER BY sale_amount DESC;


--(Milestone 4, Task 6) which months of each year produced the highest amount of sales

SELECT ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric,2) AS total_sales,
		dim_date_times.year AS year,
		dim_date_times.month AS month
FROM orders_table
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;


--(Milestone 4, Task 7) Staff headcount per world location
SELECT SUM(staff_numbers) AS total_staff_numbers,
		country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;


--(Milestone 4, Task 8) Which german stores are selling the most

SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric,2) AS total_sales,
		dim_store_details.store_type,
		dim_store_details.country_code		
FROM orders_table
INNER JOIN dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY dim_store_details.store_type, dim_store_details.country_code
HAVING country_code = 'DE'
ORDER BY total_sales ASC;


-- (Milestone 4, Task 9) How quickly is the company making sales: average time taken between sales each year

WITH time_taken_table AS (SELECT year,
		--date_time,
		--LEAD(date_time, 1) OVER(ORDER BY date_time) AS next_time_taken,
		LEAD(date_time, 1) OVER (ORDER BY date_time) - date_time AS time_taken
FROM dim_date_times)

SELECT year,
		concat('hours: ', cast(ROUND(AVG(EXTRACT(HOUR from time_taken)), 2) as text),
		' , minutes: ', cast(ROUND(AVG(EXTRACT(MINUTE from time_taken)), 2) as text), 
		' , seconds: ', cast(ROUND(AVG(EXTRACT(SECOND from time_taken)), 2) as text),
		' , milliseconds: ', cast(ROUND(AVG(EXTRACT(MILLISECOND from time_taken)), 2) as text)) AS actual_time_taken
FROM time_taken_table
GROUP BY year
ORDER BY actual_time_taken DESC;

--
--view tables
SELECT *
FROM dim_card_details;
SELECT *
FROM dim_date_times;
SELECT *
FROM dim_products;
SELECT *
FROM dim_store_details;
SELECT *
FROM dim_users;
SELECT *
FROM orders_table;

	
	
	
	
	
	
	




--
--



