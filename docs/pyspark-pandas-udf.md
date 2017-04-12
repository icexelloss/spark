Pyspark Pandas Udf
==================
split-apply-merge is a useful pattern when analyzing data. It is implemented in many popular data analying libraries such as Spark, Pandas, R, and etc. Split and merge operations in these libraries are similar to each other, mostly implemented by a `group by` operator. For instance, Spark DataFrame has groupBy, Pandas DataFrame has groupby. Therefore, for users familiar with either Spark DataFrame or pandas DataFrame, it is not difficult for them to understand how grouping works in the other library. However, `apply` is more native to different libraries and therefore, quite different between libraries. A pandas user knows how to use `apply` to do a linear regression on the grouped data might not know how to do the same calculation using pyspark, for instance. Also, the current implementation of passing data from the java executor to python executor is not effiecient, there is oppurtunity to speed it up using Apache Arrow. We are proposing new functions that allows easy and high performance split-apply-merge computation using pyspark and pandas.

Related Work
============
SPARK-13534

This enables faster data serialization between Pyspark and Pandas using Apache Arrow. Our work will be on top of this and use the same serialization for pandas udf.

SPARK-12919 and SPARK-12922

These implemented two functions: dapply and gapply in Spark R which implements the similar split-apply-merge pattern that we want to implement with Pyspark.

API
===
We propose adding a pandas udf function:

```
from pyspark.sql.functions import pandas_udf

# This must match the returned pandas DataFrame of my_pandas_fn
schema = StructType([StructField("v1", DoubleType(), True), ...])
my_pandas_fn = lambda df: ...

df.groupby("id").apply(pandas_udf(my_pandas_fn: ...,  schema))
```
This will apply my_pandas_fn on each group and combine the results together into a pyspark DataFrame.

and

```
from pyspark.sql.functions import pandas_udf
schema = StructType([StructField("v1", DoubleType(), True), ...])
my_pandas_fn = lambda df: ...

df.apply(pandas_udf(my_pandas_fn: ...,  schema))

```
This will apply my_pandas_fn on each partition and combine results together in the order of partitions.
