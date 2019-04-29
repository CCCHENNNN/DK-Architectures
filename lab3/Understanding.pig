-- Load data
A = LOAD '/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv' USING PigStorage(',') AS (userId:int, movieId:int, rating:float, timestamp:long);

-- Make step-by-step execution of a sequence of statements
ILLUSTRATE A;

-- Filter the tupels with the rating which are between 0.5 and 5.0
B = FILTER A BY rating>0.5 AND rating<5.0;
ILLUSTRATE B;

-- To view the logical, physical, or MapReduce execution plans to compute a relation.
EXPLAIN B;

-- Group the data in one relation. It collects the data having the same userId.
C = GROUP B BY userId;
ILLUSTRATE C;

-- Generate the avarage rating of B based on the userId 
D = FOREACH C GENERATE group AS userId, AVG(B.rating) AS avgRating;
ILLUSTRATE D;

-- Sort the data in one relation. It sorts the data by avgRating in the contrary order
E = ORDER D BY avgRating DESC;

-- Show the contents of a relation on the console.
DUMP E;

EXPLAIN E;


--Using the EXPLAIN command, show a plan for line 3; explain it

/*
grunt> EXPLAIN B;
*/

/*
The command EXPLAIN can show us the logical, physical, or MapReduce execution plans.
First part and the second show the logical and physical executions
The third is MapReduce execution, we can konw that there is only mapper but not reducer during this execution
*/


-- Result
/*
2018-10-16 22:07:20,524 [main] WARN  org.apache.pig.newplan.BaseOperatorPlan - Encountered Warning IMPLICIT_CAST_TO_DOUBLE 2 time(s).
2018-10-16 22:07:20,534 [main] INFO  org.apache.hadoop.conf.Configuration.deprecation - fs.default.name is deprecated. Instead, use fs.defaultFS
2018-10-16 22:07:20,534 [main] INFO  org.apache.hadoop.conf.Configuration.deprecation - io.bytes.per.checksum is deprecated. Instead, use dfs.bytes-per-checksum
2018-10-16 22:07:20,534 [main] WARN  org.apache.pig.data.SchemaTupleBackend - SchemaTupleBackend has already been initialized
2018-10-16 22:07:20,535 [main] INFO  org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer - {RULES_ENABLED=[AddForEach, ColumnMapKeyPrune, ConstantCalculator, GroupByConstParallelSetter, LimitOptimizer, LoadTypeCastInserter, MergeFilter, MergeForEach, PartitionFilterOptimizer, PredicatePushdownOptimizer, PushDownForEachFlatten, PushUpFilter, SplitFilter, StreamTypeCastInserter]}
#-----------------------------------------------
# New Logical Plan:
#-----------------------------------------------
B: (Name: LOStore Schema: userId#323:int,movieId#324:int,rating#325:float,timestamp#326:long)
|
|---B: (Name: LOFilter Schema: userId#323:int,movieId#324:int,rating#325:float,timestamp#326:long)
    |   |
    |   (Name: And Type: boolean Uid: 350)
    |   |
    |   |---(Name: GreaterThan Type: boolean Uid: 341)
    |   |   |
    |   |   |---(Name: Cast Type: double Uid: 325)
    |   |   |   |
    |   |   |   |---rating:(Name: Project Type: float Uid: 325 Input: 0 Column: 2)
    |   |   |
    |   |   |---(Name: Constant Type: double Uid: 340)
    |   |
    |   |---(Name: LessThan Type: boolean Uid: 344)
    |       |
    |       |---(Name: Cast Type: double Uid: 325)
    |       |   |
    |       |   |---rating:(Name: Project Type: float Uid: 325 Input: 0 Column: 2)
    |       |
    |       |---(Name: Constant Type: double Uid: 343)
    |
    |---A: (Name: LOForEach Schema: userId#323:int,movieId#324:int,rating#325:float,timestamp#326:long)
        |   |
        |   (Name: LOGenerate[false,false,false,false] Schema: userId#323:int,movieId#324:int,rating#325:float,timestamp#326:long)ColumnPrune:OutputUids=[323, 324, 325, 326]ColumnPrune:InputUids=[323, 324, 325, 326]
        |   |   |
        |   |   (Name: Cast Type: int Uid: 323)
        |   |   |
        |   |   |---userId:(Name: Project Type: bytearray Uid: 323 Input: 0 Column: (*))
        |   |   |
        |   |   (Name: Cast Type: int Uid: 324)
        |   |   |
        |   |   |---movieId:(Name: Project Type: bytearray Uid: 324 Input: 1 Column: (*))
        |   |   |
        |   |   (Name: Cast Type: float Uid: 325)
        |   |   |
        |   |   |---rating:(Name: Project Type: bytearray Uid: 325 Input: 2 Column: (*))
        |   |   |
        |   |   (Name: Cast Type: long Uid: 326)
        |   |   |
        |   |   |---timestamp:(Name: Project Type: bytearray Uid: 326 Input: 3 Column: (*))
        |   |
        |   |---(Name: LOInnerLoad[0] Schema: userId#323:bytearray)
        |   |
        |   |---(Name: LOInnerLoad[1] Schema: movieId#324:bytearray)
        |   |
        |   |---(Name: LOInnerLoad[2] Schema: rating#325:bytearray)
        |   |
        |   |---(Name: LOInnerLoad[3] Schema: timestamp#326:bytearray)
        |
        |---A: (Name: LOLoad Schema: userId#323:bytearray,movieId#324:bytearray,rating#325:bytearray,timestamp#326:bytearray)RequiredFields:null
#-----------------------------------------------
# Physical Plan:
#-----------------------------------------------
B: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-146
|
|---B: Filter[bag] - scope-136
    |   |
    |   And[boolean] - scope-145
    |   |
    |   |---Greater Than[boolean] - scope-140
    |   |   |
    |   |   |---Cast[double] - scope-138
    |   |   |   |
    |   |   |   |---Project[float][2] - scope-137
    |   |   |
    |   |   |---Constant(0.5) - scope-139
    |   |
    |   |---Less Than[boolean] - scope-144
    |       |
    |       |---Cast[double] - scope-142
    |       |   |
    |       |   |---Project[float][2] - scope-141
    |       |
    |       |---Constant(5.0) - scope-143
    |
    |---A: New For Each(false,false,false,false)[bag] - scope-135
        |   |
        |   Cast[int] - scope-124
        |   |
        |   |---Project[bytearray][0] - scope-123
        |   |
        |   Cast[int] - scope-127
        |   |
        |   |---Project[bytearray][1] - scope-126
        |   |
        |   Cast[float] - scope-130
        |   |
        |   |---Project[bytearray][2] - scope-129
        |   |
        |   Cast[long] - scope-133
        |   |
        |   |---Project[bytearray][3] - scope-132
        |
        |---A: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv:PigStorage(',')) - scope-122

2018-10-16 22:07:20,541 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler - File concatenation threshold: 100 optimistic? false
2018-10-16 22:07:20,541 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MultiQueryOptimizer - MR plan size before optimization: 1
2018-10-16 22:07:20,541 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MultiQueryOptimizer - MR plan size after optimization: 1
#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node scope-147
Map Plan
B: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-146
|
|---B: Filter[bag] - scope-136
    |   |
    |   And[boolean] - scope-145
    |   |
    |   |---Greater Than[boolean] - scope-140
    |   |   |
    |   |   |---Cast[double] - scope-138
    |   |   |   |
    |   |   |   |---Project[float][2] - scope-137
    |   |   |
    |   |   |---Constant(0.5) - scope-139
    |   |
    |   |---Less Than[boolean] - scope-144
    |       |
    |       |---Cast[double] - scope-142
    |       |   |
    |       |   |---Project[float][2] - scope-141
    |       |
    |       |---Constant(5.0) - scope-143
    |
    |---A: New For Each(false,false,false,false)[bag] - scope-135
        |   |
        |   Cast[int] - scope-124
        |   |
        |   |---Project[bytearray][0] - scope-123
        |   |
        |   Cast[int] - scope-127
        |   |
        |   |---Project[bytearray][1] - scope-126
        |   |
        |   Cast[float] - scope-130
        |   |
        |   |---Project[bytearray][2] - scope-129
        |   |
        |   Cast[long] - scope-133
        |   |
        |   |---Project[bytearray][3] - scope-132
        |
        |---A: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv:PigStorage(',')) - scope-122--------
Global sort: false
----------------
*/