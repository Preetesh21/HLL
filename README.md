# Efficient Cardinality Estimation

This project showcases my attempt to solve the issue of efficiently estimating the cardinality of a multiset using the HyperLogLog Algorithm.

## HyperLogLog Algorithm

Traditional methods of counting would calculate the exact number of unique items in the set but would need O(n) of space and time. So HLL Algorithm instead of calculating, estimates the number of distinct items in the set.

### WORKING

The assumption that HLL makes is that the set is composed of uniformly distributed random numbers. So HLL hashes the input such that it becomes uniform and randomly distributed and then take their binary representations and counts the number of 0's at the end.

Reason---Assume that you have a string of length m which consists of {0, 1} with equal probability. What is the probability that it will start with 0, with 2 zeros, with k zeros? It is 1/2, 1/4 and 1/2^k. This means that if you have encountered a string with k zeros, you have looked through 2^k elements. 
So, we could also say that since the probability is 2^k then the total number of elements in the set would be close to 2^k. If the algorithm worked in this state, then there would be a lot of variances and not particularly good.

So, to reduce the error multiple hashing was one of the solutions but since hashing is an expensive operation so some other work around was needed to be done. To reduce the error, it was proposed that the algorithm should divide the input into **buckets/nodes/sketches** and performs operations on each of them and then takes the harmonic average of these results to compute the result. To make this division into **sketches/buckets** what the algorithm does is that it takes first m bits of the hashed binary form of the input and based on these values assign them to a bucket of values and then the remaining bits to calculate the 0's and estimating the cardinality of the bucket.
By having m buckets, we are simulating a situation in which we had m different hash functions. This costs us nothing in terms of accuracy but saves us from having to compute many independent hash functions. This procedure is called **stochastic averaging**. 

The algorithm provides an accuracy in the excess of 98% for a dataset of over 1 billion rows while using less than 1.5Kb of storage space.
The main application of this algorithm could be found in situations such as where a nearly accurate result is good enough such as estimating the traffic in the city through the car count or the spread of disease through patient count. The algorithm is used by Google to monitor the search results and by Facebook to estimate the active users.

## Data Structures 

There were two main data structures to consider from Accumulators and UDAF's and in this project I have attempted to implement both of them and check their respective performances with the accurate result.

### UDAF

As part of any data analysis workflow, doing some sort of aggregation across groups, or columns is common. 
These work exactly like aggregators except for the fact that these are not optimized and might run for a longer duration.
One can create a User Defined Aggregate Function in Spark by extending the following ‘UserDefinedAggregationFunction’ class present in the package ‘org.apache.spark.sql.expressions’ and overriding the implementation of the functions such as initialize, update, merge, evaluate.

### Accumulators

They are shared variables provided to the executor nodes where the operations are performed, and they store the values and in the driver node their values are merged, and final operations could be done. Then there accumulatorV2 API which is an abstraction of accumulators.
The AccumulatorV2 API in spark enables you to define clean custom accumulators for stats for your job. The AccumulatorV2 abstract class has several methods which need to override reset for resetting the accumulator to zero, and add for add another value into the accumulator, merge for merging another same-type accumulator into this one.

## Solution Approach

So, there are two main parameters to keep in mind while thinking of the solution for the efficient cardinality estimation **privacy** and **performance**.

The privacy concnern with respect to the problem statement is that Spark displays the content of accumulators with their count in the Yarn UI/logs. This was challenging as Users/entities are sensitive information and should not be leaked.  
The performance on using the HLL would not be 100% and there could possibly be an error of around 1% when the data entries are in the excess of 1 billion. Thus, using HLL means compromising on a bit of accuracy to be more efficient in terms of space complexity.
Another major point to keep in mind is the persistence of data. Every I/O operation is very costly so adding a new stage in the pipeline would not be very suitable, so we need to take care of this as well.

So as part of the solution we would the following steps were thought of:
   
   1) Generating Datasets to test on

First need to be able to generate Random data for unique user count (meaningful dataset generation). There are three kinds of dataset that could be considered for testing the algorithm such as:

- Uniformly distributed users’ dataset
- Skewly distribution user dataset
- An already published dataset that can be useful

2) Choosing the Data Structure and implementation of HLL

The following are the two approaches that we could take:

**Solution 1 Approach**

Define the internal data structure to store the data and expose only the count to the user with the HLL algorithm used in backdrop either from spark or from some other open-source libraries. Here we can use any of the data structures described above such as UDAF’s, accumulators or even standalone implementations of HLL.

**Solution 2 Approach**

Define Map Accumulator for unique user counting. Define the internal data structure to store the data and expose only the count to the user. Thereby keeping the privacy issue checked.

There are two open-source implementations found for the HLL algorithm             sql-alchemy and stream-lib. Both implementations are open source and could be attempted to integrate with UDAF's or accumulators. The main difference in the implementation is in the hash function that these two algorithms use.

The implementation provided by spark approx_distinct_count is an extension of the imperative aggregate class and only intakes SQL type expressions. 


3)	Wrapping everything in a main function

Then have the main class defined which wraps everything and then outputs the results to the user. The main class shall take input the dataset and output the unique user ID’s present in it using any one of the two approaches described above.

4)	Testing of the code

Write well established test suite having unit tests covering all the scenarios.


## Project Strucuture

```
project
    ├── pom.xml
    ├── src
    │   └── main
    │       ├── resources
    │       │   ├── sample.csv
    │       │   └── work_leave.csv
    │       └── scala
    │           └── com
    │               ├── important
    │               │   ├── Dataset_Generator.scala
    │               │   ├── HLLAccumulator.scala
    │               │   ├── hll_spark_inbuilt.scala
    │               │   ├── HyperLogLog_defined_by_me.scala
    │               │   ├── HyperLogLogPlusAggregator.scala
    │               │   ├── using_hll_accumulator.scala
    │               │   ├── using_hll_defined_by_me.scala
    │               │   ├── mains.scala
    │               │   └── using_hll_udaf.scala
    │               │   ├── MapAccumulator.scala
    │               │   ├── hll_spark_inbuilt.scala
    │               │   └── using_map_accumulator.scala
    |    
    │               └── others
    │                   ├── count_rows_using_udaf.scala
    │                   ├── row_count_using_count.scala
    │                   ├── simple_accumulator_attempt.scala
    │                   ├── UDAF_Calculate.scala
    │                   └── udf_email_validator.scala
    └── target
        ├── classes
        |   ├────
        ├── maven-archiver
        │   └── pom.properties
        └── untitled3-1.0-SNAPSHOT.jar


```

Here I have created two packages **com.important** and **com.others** with the main files being present in the former one and the latter one containing all the non essential files written by me in the course of building the project.

Now going through the files in the **com.important** ::

**Dataset_Generator.scala**- This file contains the code which is capable of producing datasets which could either be uniformly distributed or could be skewed based upon the arguments passed to it. (It takes number or rows,number of columns and the type as arguments)

**HLLAccumulator.scala**- This file contains the code for HLL Algorithm used from an open source library and integrated with AccumulatorV2 API of Spark.

**hll_spark_inbuilt.scala**- This file contains the code for the usage of spark's inbuilt functionality for HLL Algorithm.

**HyperLogLog_defined_by_me.scala**- This file contains the class for the personal implementation of the HLL ALgorithm.

**HyperLogLogPlusAggregator.scala**- This file contains the code for HLL Algorithm used from an open source library and integrated with UDAF class of Spark.

**mains.scala**- This file is sort of a main class which takes input the dataset path from the user and then the solution approach i.e. UDAF or accumulators based and then whether to use HLL or not. It outputs the result to teh user.

**MapAccumulator.scala**- This file contains the code for Map Accumulator used to count the unique ID's in the dataset. This gives the exact correct ans for the dataset.

**using_hll_accumulator.scala**- This file contains the implementation of usage of the accumulator defined in the **HLLAccumulator.scala** .

**using_hll_defined_by_me**.scala- This file contains the implementation of usage of the HLL class defined in the **hll_defined_by_me.scala** .

**using_hll_udaf.scala**- This file contains the implementation of usage of the udaf defined in the **HyperLogLogPlusAggregator.scala** .

**using_map_accumulator**- This file contains the implementation of usage of the map accumulator defined in the **MapAccumulator.scala**

**hll_spark_inbuilt.scala**- This file contains the implementtion of usage of the spark inbuilt function to compute the unique count.