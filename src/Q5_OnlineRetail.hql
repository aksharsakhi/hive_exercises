-- Q5: Online Retail Dataset

CREATE DATABASE IF NOT EXISTS retail_db;
USE retail_db;

-- Create OnlineRetail table
-- Dataset Columns: InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
CREATE TABLE IF NOT EXISTS online_retail (
    InvoiceNo STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate STRING,
    UnitPrice DOUBLE,
    CustomerID STRING,
    Country STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '../inputs/OnlineRetail.csv' INTO TABLE online_retail;

-- Create CustomerDetails table
CREATE TABLE IF NOT EXISTS customer_details (
    CustomerID STRING,
    CustomerName STRING,
    City STRING,
    MembershipType STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../inputs/CustomerDetails.csv' INTO TABLE customer_details;


-- (i) Display products with UnitPrice greater than 50.
SELECT DISTINCT Description, UnitPrice 
FROM online_retail 
WHERE UnitPrice > 50;

-- (ii) Calculate the average UnitPrice.
SELECT AVG(UnitPrice) AS avg_unit_price 
FROM online_retail;

-- (iii) Find Maximum UnitPrice and Minimum UnitPrice
SELECT MAX(UnitPrice) AS max_price, MIN(UnitPrice) AS min_price 
FROM online_retail;

-- (iv) Display country-wise customer count.
SELECT Country, COUNT(DISTINCT CustomerID) AS customer_count 
FROM online_retail 
GROUP BY Country;

-- (v) Display product-wise revenue. (Revenue = Quantity * UnitPrice)
SELECT Description, SUM(Quantity * UnitPrice) AS total_revenue 
FROM online_retail 
GROUP BY Description;

-- (vi) Display countries having sales greater than £500,000.
SELECT Country, SUM(Quantity * UnitPrice) AS total_sales 
FROM online_retail 
GROUP BY Country 
HAVING SUM(Quantity * UnitPrice) > 500000;

-- (vii) Display the top 20 customers based on purchase amount.
SELECT CustomerID, SUM(Quantity * UnitPrice) AS purchase_amount 
FROM online_retail 
WHERE CustomerID IS NOT NULL AND CustomerID != ''
GROUP BY CustomerID 
ORDER BY purchase_amount DESC 
LIMIT 20;

-- (viii) Display the length of each product description.
SELECT DISTINCT Description, length(Description) AS desc_length 
FROM online_retail 
WHERE Description IS NOT NULL;

-- (ix) Display monthly sales summary.
-- Assuming InvoiceDate is in format "MM/dd/yy HH:mm" or similar, we extract the month and year
-- Let's extract the month/year simply. If it's standard string like '12/1/2010 8:26', we can split.
-- We will just group by the substring representing the month for simplicity, assuming a standard datetime function.
SELECT substr(InvoiceDate, 1, 7) AS month_year, SUM(Quantity * UnitPrice) AS monthly_sales
FROM online_retail
GROUP BY substr(InvoiceDate, 1, 7)
ORDER BY month_year;

-- (x) Create a partitioned Hive table based on Country.
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE IF NOT EXISTS online_retail_partitioned (
    InvoiceNo STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate STRING,
    UnitPrice DOUBLE,
    CustomerID STRING
)
PARTITIONED BY (Country STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE online_retail_partitioned PARTITION(Country)
SELECT InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country 
FROM online_retail;

-- (xi) Perform an INNER JOIN between OnlineRetail and CustomerDetails. List Premium members and their purchase amounts.
SELECT c.CustomerName, c.MembershipType, SUM(o.Quantity * o.UnitPrice) AS total_purchase
FROM online_retail o
JOIN customer_details c ON o.CustomerID = c.CustomerID
WHERE c.MembershipType = 'Premium'
GROUP BY c.CustomerName, c.MembershipType;

-- (xii) Identify the customer contributing the highest revenue in each country.
WITH CountryCustomerRevenue AS (
    SELECT Country, CustomerID, SUM(Quantity * UnitPrice) as revenue
    FROM online_retail
    WHERE CustomerID IS NOT NULL AND CustomerID != ''
    GROUP BY Country, CustomerID
),
RankedCustomers AS (
    SELECT Country, CustomerID, revenue,
           ROW_NUMBER() OVER(PARTITION BY Country ORDER BY revenue DESC) as rank
    FROM CountryCustomerRevenue
)
SELECT Country, CustomerID, revenue 
FROM RankedCustomers 
WHERE rank = 1;

-- (xiii) Generate a monthly business report
SELECT 
    substr(InvoiceDate, 1, 7) AS month_year,
    SUM(Quantity * UnitPrice) AS Total_Revenue,
    COUNT(DISTINCT InvoiceNo) AS Number_of_Orders,
    COUNT(DISTINCT CustomerID) AS Number_of_Customers,
    SUM(Quantity * UnitPrice) / COUNT(DISTINCT InvoiceNo) AS Average_Order_Value
FROM online_retail
GROUP BY substr(InvoiceDate, 1, 7)
ORDER BY month_year;

-- (xiv) Identify customers who purchased products from more than five different categories
-- Since 'Category' is not available, we assume 'StockCode' represents a unique product category.
SELECT CustomerID, COUNT(DISTINCT StockCode) AS category_count
FROM online_retail
WHERE CustomerID IS NOT NULL AND CustomerID != ''
GROUP BY CustomerID
HAVING COUNT(DISTINCT StockCode) > 5;

-- (xv) Create a final Hive report listing: Country, Total Revenue, Total Orders, Average Order Value, Top Customer, Top Selling Product.
WITH CountryAgg AS (
    SELECT Country, 
           SUM(Quantity * UnitPrice) as total_revenue,
           COUNT(DISTINCT InvoiceNo) as total_orders
    FROM online_retail
    GROUP BY Country
),
TopCustomer AS (
    SELECT Country, CustomerID, revenue
    FROM (
        SELECT Country, CustomerID, SUM(Quantity * UnitPrice) as revenue,
               ROW_NUMBER() OVER(PARTITION BY Country ORDER BY SUM(Quantity * UnitPrice) DESC) as rank
        FROM online_retail
        WHERE CustomerID IS NOT NULL AND CustomerID != ''
        GROUP BY Country, CustomerID
    ) t WHERE rank = 1
),
TopProduct AS (
    SELECT Country, Description, qty
    FROM (
        SELECT Country, Description, SUM(Quantity) as qty,
               ROW_NUMBER() OVER(PARTITION BY Country ORDER BY SUM(Quantity) DESC) as rank
        FROM online_retail
        WHERE Description IS NOT NULL
        GROUP BY Country, Description
    ) p WHERE rank = 1
)
SELECT 
    ca.Country,
    ca.total_revenue AS Total_Revenue,
    ca.total_orders AS Total_Orders,
    ca.total_revenue / ca.total_orders AS Average_Order_Value,
    tc.CustomerID AS Top_Customer,
    tp.Description AS Top_Selling_Product
FROM CountryAgg ca
LEFT JOIN TopCustomer tc ON ca.Country = tc.Country
LEFT JOIN TopProduct tp ON ca.Country = tp.Country;
