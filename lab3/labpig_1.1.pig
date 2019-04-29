-- 1.1 Show all users which have more than 100 reviews
ratings = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);

users = GROUP ratings BY userId;
reviews = FOREACH users GENERATE group AS userId, COUNT(ratings.movieId) AS nbReview;
result = FILTER reviews BY nbReview > 100;
ILLUSTRATE result;
DUMP result;
EXPLAIN result;