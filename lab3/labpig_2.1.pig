-- 2.1 The average rating for ’Documentary’ movies
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