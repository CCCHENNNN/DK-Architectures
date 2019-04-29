1.1

For this question, firstly "ratings" from loading has a mapper, then the function "GROUP BY" gets "users" by a reducer. For the review, the "COUNT()" there is a combiner for the local data. In the end it uses reducer to get the "result".

#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node scope-704
Map Plan
users: Local Rearrange[tuple]{int}(false) - scope-717
|   |
|   Project[int][0] - scope-719
|
|---reviews: New For Each(false,false)[bag] - scope-705
    |   |
    |   Project[int][0] - scope-706
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Initial)[tuple] - scope-707
    |   |
    |   |---Project[bag][1] - scope-708
    |       |
    |       |---Project[bag][1] - scope-709
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-720
        |
        |---ratings: New For Each(false,false,false,false)[bag] - scope-687
            |   |
            |   Cast[int] - scope-676
            |   |
            |   |---Project[bytearray][0] - scope-675
            |   |
            |   Cast[int] - scope-679
            |   |
            |   |---Project[bytearray][1] - scope-678
            |   |
            |   Cast[float] - scope-682
            |   |
            |   |---Project[bytearray][2] - scope-681
            |   |
            |   Cast[long] - scope-685
            |   |
            |   |---Project[bytearray][3] - scope-684
            |
            |---ratings: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv:PigStorage(',')) - scope-674--------
Combine Plan
users: Local Rearrange[tuple]{int}(false) - scope-721
|   |
|   Project[int][0] - scope-723
|
|---reviews: New For Each(false,false)[bag] - scope-710
    |   |
    |   Project[int][0] - scope-711
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Intermediate)[tuple] - scope-712
    |   |
    |   |---Project[bag][1] - scope-713
    |
    |---users: Package(CombinerPackager)[tuple]{int} - scope-716--------
Reduce Plan
result: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-703
|
|---result: Filter[bag] - scope-699
    |   |
    |   Greater Than[boolean] - scope-702
    |   |
    |   |---Project[long][1] - scope-700
    |   |
    |   |---Constant(100) - scope-701
    |
    |---reviews: New For Each(false,false)[bag] - scope-698
        |   |
        |   Project[int][0] - scope-692
        |   |
        |   POUserFunc(org.apache.pig.builtin.COUNT$Final)[long] - scope-696
        |   |
        |   |---Project[bag][1] - scope-714
        |
        |---users: Package(CombinerPackager)[tuple]{int} - scope-689--------
Global sort: false
----------------


1.2

This question is same than the question 1.1, Mapper, Combiner and Reducer. "result" gets the data from different combiners and be put into the reducer. 


#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node scope-842
Map Plan
movieReview: Local Rearrange[tuple]{int}(false) - scope-855
|   |
|   Project[int][0] - scope-857
|
|---result: New For Each(false,false)[bag] - scope-843
    |   |
    |   Project[int][0] - scope-844
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Initial)[tuple] - scope-845
    |   |
    |   |---Project[bag][2] - scope-846
    |       |
    |       |---Project[bag][1] - scope-847
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-858
        |
        |---ratings: New For Each(false,false,false,false)[bag] - scope-829
            |   |
            |   Cast[int] - scope-818
            |   |
            |   |---Project[bytearray][0] - scope-817
            |   |
            |   Cast[int] - scope-821
            |   |
            |   |---Project[bytearray][1] - scope-820
            |   |
            |   Cast[float] - scope-824
            |   |
            |   |---Project[bytearray][2] - scope-823
            |   |
            |   Cast[long] - scope-827
            |   |
            |   |---Project[bytearray][3] - scope-826
            |
            |---ratings: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv:PigStorage(',')) - scope-816--------
Combine Plan
movieReview: Local Rearrange[tuple]{int}(false) - scope-859
|   |
|   Project[int][0] - scope-861
|
|---result: New For Each(false,false)[bag] - scope-848
    |   |
    |   Project[int][0] - scope-849
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Intermediate)[tuple] - scope-850
    |   |
    |   |---Project[bag][1] - scope-851
    |
    |---movieReview: Package(CombinerPackager)[tuple]{int} - scope-854--------
Reduce Plan
result: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-841
|
|---result: New For Each(false,false)[bag] - scope-840
    |   |
    |   Project[int][0] - scope-834
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Final)[long] - scope-838
    |   |
    |   |---Project[bag][1] - scope-852
    |
    |---movieReview: Package(CombinerPackager)[tuple]{int} - scope-831--------
Global sort: false
----------------



2.1

There are 2 parts: Mapper + Reducer and Mapper + Combiner + Reducer. For the first part, it uses mapper to realise "LOAD" and "FILTER", then the reducer is for "movieRating" and "newOne"("JOIN" and "GENERATE"). For the second part, it's same than question 1.1 and 1.2, mapper and reducer for "GROUP" and then combiner for "COUNT", and a final reducer for "result".

#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node scope-3415
Map Plan
Union[tuple] - scope-3416
|
|---movieRating: Local Rearrange[tuple]{int}(false) - scope-3391
|   |   |
|   |   Project[int][0] - scope-3392
|   |
|   |---docs: New For Each(false)[bag] - scope-3378
|       |   |
|       |   Project[int][0] - scope-3376
|       |
|       |---docs: Filter[bag] - scope-3372
|           |   |
|           |   Matches - scope-3375
|           |   |
|           |   |---Project[chararray][1] - scope-3373
|           |   |
|           |   |---Constant(.*Documentary.*) - scope-3374
|           |
|           |---movies: New For Each(false,false)[bag] - scope-3371
|               |   |
|               |   Cast[int] - scope-3366
|               |   |
|               |   |---Project[bytearray][0] - scope-3365
|               |   |
|               |   Cast[chararray] - scope-3369
|               |   |
|               |   |---Project[bytearray][1] - scope-3368
|               |
|               |---movies: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/movies.csv:PigStorage(',')) - scope-3364
|
|---movieRating: Local Rearrange[tuple]{int}(false) - scope-3393
    |   |
    |   Project[int][0] - scope-3394
    |
    |---ratings: New For Each(false,false)[bag] - scope-3386
        |   |
        |   Cast[int] - scope-3381
        |   |
        |   |---Project[bytearray][0] - scope-3380
        |   |
        |   Cast[float] - scope-3384
        |   |
        |   |---Project[bytearray][1] - scope-3383
        |
        |---ratings: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/ratings.csv:PigStorage(',')) - scope-3379--------
Reduce Plan
Store(file:/tmp/temp1581669618/tmp1014817914:org.apache.pig.impl.io.InterStorage) - scope-3417
|
|---newOne: New For Each(false,false)[bag] - scope-3402
    |   |
    |   Project[int][0] - scope-3398
    |   |
    |   Project[float][2] - scope-3400
    |
    |---movieRating: Package(JoinPackager(true,true))[tuple]{int} - scope-3390--------
Global sort: false
----------------

MapReduce node scope-3419
Map Plan
allRating: Local Rearrange[tuple]{chararray}(false) - scope-3432
|   |
|   Project[chararray][0] - scope-3434
|
|---result: New For Each(false,false)[bag] - scope-3420
    |   |
    |   Project[chararray][0] - scope-3421
    |   |
    |   POUserFunc(org.apache.pig.builtin.FloatAvg$Initial)[tuple] - scope-3422
    |   |
    |   |---Project[bag][1] - scope-3423
    |       |
    |       |---Project[bag][1] - scope-3424
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-3435
        |
        |---Load(file:/tmp/temp1581669618/tmp1014817914:org.apache.pig.impl.io.InterStorage) - scope-3418--------
Combine Plan
allRating: Local Rearrange[tuple]{chararray}(false) - scope-3436
|   |
|   Project[chararray][0] - scope-3438
|
|---result: New For Each(false,false)[bag] - scope-3425
    |   |
    |   Project[chararray][0] - scope-3426
    |   |
    |   POUserFunc(org.apache.pig.builtin.FloatAvg$Intermediate)[tuple] - scope-3427
    |   |
    |   |---Project[bag][1] - scope-3428
    |
    |---allRating: Package(CombinerPackager)[tuple]{chararray} - scope-3431--------
Reduce Plan
result: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-3412
|
|---result: New For Each(false)[bag] - scope-3411
    |   |
    |   POUserFunc(org.apache.pig.builtin.FloatAvg$Final)[double] - scope-3409
    |   |
    |   |---Project[bag][1] - scope-3429
    |
    |---allRating: Package(CombinerPackager)[tuple]{chararray} - scope-3404--------
Global sort: false
----------------

2.2

There are 3 parts: Mapper + Reducer, Mapper + Reducer and Mapper + Combiner + Reducer. For the first part, it uses mapper to realise "LOAD" and "FILTER", then the reducer is for "movieTag" and "newOne"("JOIN" and "GENERATE"). For the second part, because it's a "DISTINCT", so we need a mapper and a reducer to realise it. Then for the third part, it's same than question 1.1 and 1.2, mapper and reducer for "GROUP" and then combiner for "COUNT", and a final reducer for "result".

#--------------------------------------------------
# Map Reduce Plan
#--------------------------------------------------
MapReduce node scope-4293
Map Plan
Union[tuple] - scope-4294
|
|---movieTag: Local Rearrange[tuple]{int}(false) - scope-4266
|   |   |
|   |   Project[int][0] - scope-4267
|   |
|   |---actions: New For Each(false)[bag] - scope-4253
|       |   |
|       |   Project[int][0] - scope-4251
|       |
|       |---actions: Filter[bag] - scope-4247
|           |   |
|           |   Matches - scope-4250
|           |   |
|           |   |---Project[chararray][1] - scope-4248
|           |   |
|           |   |---Constant(.*Action.*) - scope-4249
|           |
|           |---movies: New For Each(false,false)[bag] - scope-4246
|               |   |
|               |   Cast[int] - scope-4241
|               |   |
|               |   |---Project[bytearray][0] - scope-4240
|               |   |
|               |   Cast[chararray] - scope-4244
|               |   |
|               |   |---Project[bytearray][1] - scope-4243
|               |
|               |---movies: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/movies.csv:PigStorage(',')) - scope-4239
|
|---movieTag: Local Rearrange[tuple]{int}(false) - scope-4268
    |   |
    |   Project[int][0] - scope-4269
    |
    |---tags: New For Each(false,false)[bag] - scope-4261
        |   |
        |   Cast[int] - scope-4256
        |   |
        |   |---Project[bytearray][0] - scope-4255
        |   |
        |   Cast[chararray] - scope-4259
        |   |
        |   |---Project[bytearray][1] - scope-4258
        |
        |---tags: Load(/Users/hchen/Desktop/DK/Architectures/lab3/ml-20m/tags.csv:PigStorage(',')) - scope-4254--------
Reduce Plan
Store(file:/tmp/temp-788128066/tmp142681796:org.apache.pig.impl.io.InterStorage) - scope-4297
|
|---newOne: New For Each(false,false)[bag] - scope-4277
    |   |
    |   Project[int][0] - scope-4273
    |   |
    |   Project[chararray][2] - scope-4275
    |
    |---movieTag: Package(JoinPackager(true,true))[tuple]{int} - scope-4265--------
Global sort: false
----------------

MapReduce node scope-4299
Map Plan
Local Rearrange[tuple]{tuple}(true) - scope-4296
|   |
|   Project[tuple][*] - scope-4295
|
|---Load(file:/tmp/temp-788128066/tmp142681796:org.apache.pig.impl.io.InterStorage) - scope-4298--------
Reduce Plan
Store(file:/tmp/temp-788128066/tmp-20486779:org.apache.pig.impl.io.InterStorage) - scope-4303
|
|---New For Each(true)[bag] - scope-4302
    |   |
    |   Project[tuple][0] - scope-4301
    |
    |---Package(Packager)[tuple]{tuple} - scope-4300--------
Global sort: false
----------------

MapReduce node scope-4305
Map Plan
newNewNewOne: Local Rearrange[tuple]{int}(false) - scope-4318
|   |
|   Project[int][0] - scope-4320
|
|---result: New For Each(false,false)[bag] - scope-4306
    |   |
    |   Project[int][0] - scope-4307
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Initial)[tuple] - scope-4308
    |   |
    |   |---Project[bag][1] - scope-4309
    |       |
    |       |---Project[bag][1] - scope-4310
    |
    |---Pre Combiner Local Rearrange[tuple]{Unknown} - scope-4321
        |
        |---Load(file:/tmp/temp-788128066/tmp-20486779:org.apache.pig.impl.io.InterStorage) - scope-4304--------
Combine Plan
newNewNewOne: Local Rearrange[tuple]{int}(false) - scope-4322
|   |
|   Project[int][0] - scope-4324
|
|---result: New For Each(false,false)[bag] - scope-4311
    |   |
    |   Project[int][0] - scope-4312
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Intermediate)[tuple] - scope-4313
    |   |
    |   |---Project[bag][1] - scope-4314
    |
    |---newNewNewOne: Package(CombinerPackager)[tuple]{int} - scope-4317--------
Reduce Plan
result: Store(fakefile:org.apache.pig.builtin.PigStorage) - scope-4290
|
|---result: New For Each(false,false)[bag] - scope-4289
    |   |
    |   Project[int][0] - scope-4283
    |   |
    |   POUserFunc(org.apache.pig.builtin.COUNT$Final)[long] - scope-4287
    |   |
    |   |---Project[bag][1] - scope-4315
    |
    |---newNewNewOne: Package(CombinerPackager)[tuple]{int} - scope-4280--------
Global sort: false
----------------