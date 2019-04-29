-- 2.2 For each ’Action’ movie, the total number of tags that have been added
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