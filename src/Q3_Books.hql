-- Q3: Books

CREATE DATABASE IF NOT EXISTS library_db;
USE library_db;

-- 1. Create a table called books with 3 columns
CREATE TABLE IF NOT EXISTS books (
    id INT,
    title STRING,
    publishDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Create purchases table (implied)
CREATE TABLE IF NOT EXISTS purchases (
    id INT,
    buyer STRING,
    purchaseDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 2. Load data from books.txt (stored locally) into books table
LOAD DATA LOCAL INPATH '../inputs/books.txt' INTO TABLE books;
LOAD DATA LOCAL INPATH '../inputs/purchases.txt' INTO TABLE purchases;

-- 3. Move the table from local to HDFS (Wait, Hive handles this automatically for managed tables during LOAD DATA.
-- To explicitly show HDFS command, one could use `dfs -put` but doing LOAD DATA moves the file to HDFS).
-- The LOAD DATA command above fulfills this requirement.

-- 4. Select the top 4 records.
SELECT * FROM books LIMIT 4;

-- 5. Create books_purchases table
CREATE TABLE IF NOT EXISTS books_purchases (
    id INT,
    title STRING,
    buyer STRING,
    purchaseDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- 6. Populate books_purchases table by joining books and purchases tables via id column
INSERT OVERWRITE TABLE books_purchases
SELECT b.id, b.title, p.buyer, p.purchaseDate 
FROM books b 
JOIN purchases p ON b.id = p.id;

-- 7. Select title, buyer based on purchase date (e.g. order by purchaseDate)
SELECT title, buyer, purchaseDate 
FROM books_purchases 
ORDER BY purchaseDate;

-- 8. Partition the table based on buyer.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE IF NOT EXISTS books_purchases_partitioned (
    id INT,
    title STRING,
    purchaseDate STRING
)
PARTITIONED BY (buyer STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE books_purchases_partitioned PARTITION(buyer)
SELECT id, title, purchaseDate, buyer FROM books_purchases;
