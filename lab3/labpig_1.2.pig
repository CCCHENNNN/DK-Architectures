-- 1.2 Show the total number of reviews for each movie
ratings = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);

movieReview = GROUP ratings BY movieId;
result = FOREACH movieReview GENERATE group AS movieId, COUNT(ratings.rating) AS nbReview;
ILLUSTRATE result;
DUMP result;
EXPLAIN result;