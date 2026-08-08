# Assignment 3 - Hive Queries
**Student Name:** Sheela Akshar Sakhi
**Roll Number:** CB.SC.U4CSE23547

---

## Question 1: Retail Store
**Query:**
```sql
CREATE DATABASE IF NOT EXISTS retail_store;
USE retail_store;

CREATE TABLE IF NOT EXISTS retail (
    txnno INT, custno STRING, amount DOUBLE, category STRING, product STRING, city STRING, state STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../inputs/retail_data.csv' INTO TABLE retail;

-- Find the total number of records in the table.
SELECT COUNT(*) AS total_records FROM retail;

-- Find the total no of records based on city.
SELECT city, COUNT(*) AS total_records FROM retail GROUP BY city;
```

**Screenshot of Output:**
*[Insert Screenshot Here]*

---

## Question 2: PageView
**Query:**
```sql
CREATE DATABASE IF NOT EXISTS pageview_db;
USE pageview_db;

CREATE TABLE IF NOT EXISTS pageview (
    viewtime STRING, pageurl STRING, state STRING, pvusers MAP<STRING, STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\001'
MAP KEYS TERMINATED BY '\002' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../inputs/pageview_data.txt' INTO TABLE pageview;

-- Partition the table based on pageurl
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
CREATE TABLE IF NOT EXISTS pageview_partitioned (viewtime STRING, state STRING, pvusers MAP<STRING, STRING>) PARTITIONED BY (pageurl STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
INSERT OVERWRITE TABLE pageview_partitioned PARTITION(pageurl) SELECT viewtime, state, pvusers, pageurl FROM pageview;
```

**Screenshot of Output:**
*[Insert Screenshot Here]*

---

## Question 3: Books
**Query:**
```sql
CREATE DATABASE IF NOT EXISTS library_db;
USE library_db;

CREATE TABLE IF NOT EXISTS books (id INT, title STRING, publishDate STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
CREATE TABLE IF NOT EXISTS purchases (id INT, buyer STRING, purchaseDate STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '../inputs/books.txt' INTO TABLE books;
LOAD DATA LOCAL INPATH '../inputs/purchases.txt' INTO TABLE purchases;

CREATE TABLE IF NOT EXISTS books_purchases (id INT, title STRING, buyer STRING, purchaseDate STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE books_purchases SELECT b.id, b.title, p.buyer, p.purchaseDate FROM books b JOIN purchases p ON b.id = p.id;
```

**Screenshot of Output:**
*[Insert Screenshot Here]*

---

## Question 4: MovieLens Analysis
**Query Example:**
```sql
CREATE DATABASE IF NOT EXISTS movielens_db;
USE movielens_db;
-- (Loading skipped for brevity)

-- (iv) Top 5 movies with the highest average rating.
SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title
ORDER BY avg_rating DESC
LIMIT 5;

-- (vii) Find the average rating for each genre of movies.
SELECT genre, AVG(r.rating) AS avg_genre_rating
FROM movies m
LATERAL VIEW explode(split(m.genres, '\\|')) exploded_table AS genre
JOIN ratings r ON m.movieId = r.movieId
GROUP BY genre;
```

**Screenshot of Output:**
*[Insert Screenshot Here]*

---

## Question 5: Online Retail
**MapReduce Execution Screenshot:**
*[Insert Screenshot of run.sh output for MapReduce tasks here]*

**Hive Query Example:**
```sql
CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;

-- (vi) Display countries having sales greater than £500,000.
SELECT Country, SUM(Quantity * UnitPrice) AS total_sales 
FROM online_retail 
GROUP BY Country 
HAVING SUM(Quantity * UnitPrice) > 500000;

-- (xi) Perform an INNER JOIN between OnlineRetail and CustomerDetails.
SELECT c.CustomerName, c.MembershipType, SUM(o.Quantity * o.UnitPrice) AS total_purchase
FROM online_retail o
JOIN customer_details c ON o.CustomerID = c.CustomerID
WHERE c.MembershipType = 'Premium'
GROUP BY c.CustomerName, c.MembershipType;
```

**Screenshot of Output:**
*[Insert Screenshot Here]*
