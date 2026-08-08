-- Q4: MovieLens Dataset

CREATE DATABASE IF NOT EXISTS movielens_db;
USE movielens_db;

-- Create movies table
-- movies.csv: movieId,title,genres
CREATE TABLE IF NOT EXISTS movies (
    movieId INT,
    title STRING,
    genres STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create ratings table
-- ratings.csv: userId,movieId,rating,timestamp
CREATE TABLE IF NOT EXISTS ratings (
    userId INT,
    movieId INT,
    rating DOUBLE,
    timestamp_ts BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load data
LOAD DATA LOCAL INPATH '../inputs/movies.csv' INTO TABLE movies;
LOAD DATA LOCAL INPATH '../inputs/ratings.csv' INTO TABLE ratings;

-- (i) List all movies in the dataset.
SELECT title FROM movies;

-- (ii) Count the total number of users who rated movies.
SELECT COUNT(DISTINCT userId) AS total_users FROM ratings;

-- (iii) List all movies with their average rating.
SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title;

-- (iv) Top 5 movies with the highest average rating.
-- (Filtering out movies with very few ratings for a realistic top 5, but here is a direct query)
SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title
ORDER BY avg_rating DESC
LIMIT 5;

-- (v) Find the number of ratings each movie has received.
SELECT m.title, COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title;

-- (vi) List movies that were rated by more than 50 users.
SELECT m.title, COUNT(DISTINCT r.userId) AS user_count
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title
HAVING COUNT(DISTINCT r.userId) > 50;

-- (vii) Find the average rating for each genre of movies.
SELECT m2.genre, AVG(r.rating) AS avg_genre_rating
FROM (
    SELECT movieId, genre 
    FROM movies 
    LATERAL VIEW explode(split(genres, '\\|')) exploded_table AS genre
) m2
JOIN ratings r ON m2.movieId = r.movieId
GROUP BY m2.genre;

-- (viii) List the top 10 most rated movies along with their average rating.
SELECT m.title, COUNT(r.rating) AS num_ratings, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title
ORDER BY num_ratings DESC
LIMIT 10;

-- (ix) For each genre, find the top 3 highest-rated movies.
-- Assuming we want genres separated and movies with at least 10 ratings to be realistic
WITH genre_ratings AS (
    SELECT m2.genre, m2.title, AVG(r.rating) as avg_rating, COUNT(r.rating) as num_ratings
    FROM (
        SELECT movieId, title, genre 
        FROM movies 
        LATERAL VIEW explode(split(genres, '\\|')) exploded_table AS genre
    ) m2
    JOIN ratings r ON m2.movieId = r.movieId
    GROUP BY m2.genre, m2.title
),
ranked_movies AS (
    SELECT genre, title, avg_rating,
           ROW_NUMBER() OVER(PARTITION BY genre ORDER BY avg_rating DESC) as rank
    FROM genre_ratings
    -- WHERE num_ratings > 10 (Optional: uncomment to filter movies with too few ratings)
)
SELECT genre, title, avg_rating
FROM ranked_movies
WHERE rank <= 3;

-- (x) For each user, find the movie they rated the highest.
WITH user_movie_rank AS (
    SELECT r.userId, m.title, r.rating,
           ROW_NUMBER() OVER(PARTITION BY r.userId ORDER BY r.rating DESC) as rank
    FROM ratings r
    JOIN movies m ON r.movieId = m.movieId
)
SELECT userId, title, rating
FROM user_movie_rank
WHERE rank = 1;
