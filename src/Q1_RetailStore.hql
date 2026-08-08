-- Q1: RETAIL store
-- 1. Create a database named as RETAIL store.
CREATE DATABASE IF NOT EXISTS retail_store;
USE retail_store;

-- 2. Create a table retail with the fields txnno,custno,amount,category,product,city,state.
CREATE TABLE IF NOT EXISTS retail (
    txnno INT,
    custno STRING,
    amount DOUBLE,
    category STRING,
    product STRING,
    city STRING,
    state STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 3. Load the data into table
LOAD DATA LOCAL INPATH '../inputs/retail_data.csv' INTO TABLE retail;

-- 4. Find the total number of records in the table.
SELECT COUNT(*) AS total_records FROM retail;

-- 5. Find the total no of records based on city.
SELECT city, COUNT(*) AS total_records FROM retail GROUP BY city;

-- 6. Partition the table based on category
-- Need to enable dynamic partitioning in Hive
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE IF NOT EXISTS retail_partitioned (
    txnno INT,
    custno STRING,
    amount DOUBLE,
    product STRING,
    city STRING,
    state STRING
)
PARTITIONED BY (category STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE retail_partitioned PARTITION(category)
SELECT txnno, custno, amount, product, city, state, category FROM retail;

-- 7. Create a cluster based on city.
CREATE TABLE IF NOT EXISTS retail_clustered (
    txnno INT,
    custno STRING,
    amount DOUBLE,
    category STRING,
    product STRING,
    city STRING,
    state STRING
)
CLUSTERED BY (city) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Note: we need to enforce bucketing
SET hive.enforce.bucketing = true;
INSERT OVERWRITE TABLE retail_clustered 
SELECT txnno, custno, amount, category, product, city, state FROM retail;

-- 8. Find the total amount group by category.
SELECT category, SUM(amount) AS total_amount FROM retail GROUP BY category;
