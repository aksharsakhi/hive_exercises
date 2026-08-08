-- Q2: PageView

CREATE DATABASE IF NOT EXISTS pageview_db;
USE pageview_db;

-- 1. Create a table with pageview with the following fields: (viewtime, pageurl ,state, pvusers ). Assume pvusers map type with (uid, username)
CREATE TABLE IF NOT EXISTS pageview (
    viewtime STRING,
    pageurl STRING,
    state STRING,
    pvusers MAP<STRING, STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\002' -- Assuming collection delimiter is ^B
MAP KEYS TERMINATED BY '\003' -- Note: our dummy data only has 1 pair, separated by \002
STORED AS TEXTFILE;

-- Load data (our dummy data has uid\002username, so actually map key term is \002)
DROP TABLE IF EXISTS pageview;
CREATE TABLE IF NOT EXISTS pageview (
    viewtime STRING,
    pageurl STRING,
    state STRING,
    pvusers MAP<STRING, STRING>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '\001'
MAP KEYS TERMINATED BY ':' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../inputs/pageview_data.txt' INTO TABLE pageview;

-- 2. Display the table
SELECT * FROM pageview;

-- 3. Partition the table based on pageurl
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE TABLE IF NOT EXISTS pageview_partitioned (
    viewtime STRING,
    state STRING,
    pvusers MAP<STRING, STRING>
)
PARTITIONED BY (pageurl STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

INSERT OVERWRITE TABLE pageview_partitioned PARTITION(pageurl)
SELECT viewtime, state, pvusers, pageurl FROM pageview;

-- 4. Find the total view time (Counting the total views since viewtime is a timestamp)
SELECT COUNT(viewtime) AS total_view_count FROM pageview;

-- 5. Using map uid display the other fields in table.
-- Assuming uid is the key of the map
SELECT viewtime, pageurl, state, map_keys(pvusers)[0] as uid, map_values(pvusers)[0] as username 
FROM pageview;
