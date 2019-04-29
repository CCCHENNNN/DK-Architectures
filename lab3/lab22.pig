-- 1. Using lab2.pig as a base, compute the following: 
-- Show all users which have more than 100 reviews
ratings = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);

users = GROUP ratings BY userId; map+reduce
reviews = FOREACH users GENERATE group AS userId, COUNT(ratings.movieId) AS nbReview; map+combine+reduce
result = FILTER reviews BY nbReview > 100; map+combine+reduce
ILLUSTRATE result;
DUMP result;
EXPLAIN result;

-- Show the total number of reviews for each movie
ratings = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);

movieReview = GROUP ratings BY movieId;
result = FOREACH movieReview GENERATE group AS movieId, COUNT(ratings.rating) AS nbReview;
ILLUSTRATE result;
DUMP result;
EXPLAIN result;


-- 2. Extend lab2.pig to use more than one relation (ratings.csv): using movies.csv and tags.csv compute
-- The average rating for ’Documentary’ movies
ratings = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);
movies = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray ,genres:chararray );

docs = FILTER movies BY genres matches '.*Documentary.*';
movieRating = JOIN docs BY movieId, ratings BY movieId;
newOne = FOREACH movieRating GENERATE docs::movieId AS movieId, ratings::rating AS rating; 
allRating = GROUP newOne ALL;
result = FOREACH allRating GENERATE AVG(newOne.rating) AS averageRating;
ILLUSTRATE result;
DUMP result;
EXPLAIN result;

-- For each ’Action’ movie, the total number of tags that have been added
movies = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray ,genres:chararray );
tags = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/tags.csv' USING PigStorage(',') AS (userId:int, movieId:int, tag:chararray, timestamp:long);

actions = FILTER movies BY genres matches '.*Action.*';
movieTag = JOIN actions BY movieId, tags BY movieId;
newOne = FOREACH movieTag GENERATE actions::movieId AS movieId, tags::tag AS tag;
newNewOne = distinct newOne;
newNewNewOne = GROUP newNewOne BY movieId;
result = FOREACH newNewNewOne GENERATE group AS movieId, COUNT(newNewOne.tag) AS nbTag;
ILLUSTRATE result;
DUMP result;
EXPLAIN result;









