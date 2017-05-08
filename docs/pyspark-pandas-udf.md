Pyspark Pandas Udf
==================
split-apply-merge is a useful pattern when analyzing data. It is implemented in many popular data analying libraries such as Spark, Pandas, R, and etc. Split and merge operations in these libraries are similar to each other, mostly implemented by a `group by` operator. For instance, Spark DataFrame has `groupBy`, Pandas DataFrame also has `groupby`. Therefore, for users familiar with either Spark DataFrame or pandas DataFrame, it is not difficult for them to understand how grouping works in the other library. However, `apply` is more native to different libraries and therefore, quite different between libraries. A pandas user knows how to winsorize grouped data might not know how to do the same calculation using pyspark, for instance. Also, the current implementation of passing data from the java executor to python executor is not effiecient, there is oppurtunity to speed it up using Apache Arrow. We are proposing new functions that allows easy and high performance split-apply-merge computation using pyspark and pandas.

For more context about split-apply-merge, see:
* http://pandas.pydata.org/pandas-docs/stable/groupby.html
* https://www.jstatsoft.org/article/view/v040i01/v40i01.pdf

Related Work
============
* SPARK-13534
This enables faster data serialization between Pyspark and Pandas using Apache Arrow. Our work will be on top of this and use the same serialization for pandas udf.

* SPARK-12919 and SPARK-12922
These implemented two functions: dapply and gapply in Spark R which implements the similar split-apply-merge pattern that we want to implement with Pyspark.

API
===
## withColumn (add a column to each row of the table)
#### group withColumn (winsorize, vector)
```
from scipy.stats import mstats
@pandas_udf(DoubleType())
def winsorize_udf(df):
    return mstats.winsorize(df.ix[:,0])

df.groupBy('id').withColumn('v1', winsorize_udf('v1'))

# or (more pandas)

df.withColumn('v1', df.groupBy('id').transform(winsorize_udf('v1')))

# pandas equiv:

df['v1'] = df.groupby('id')[['v1']].transform(mstats.winsorize)

# or (more spark sql):

df.withColumn('v1', winsorize_udf('v1').over(groupBy('id'))

```

#### group withColumn (weighted mean, scalar)
```
import numpy as np
@pandas_udf(DoubleType())
def weighted_mean_udf(df):
    return np.average(df.v1, weights=df.w)

df.groupBy('id').withColumn('v1_wm', weighted_mean_udf('v1', 'w'))
```

#### window withColumn (ema, scalar)
```
@pandas_udf(DoubleType())
def ema_udf(df):
    return df.ix[:,0].ewm(alpha=0.5).mean().ix[:,0].iloc[-1]

df.withColumn('v1_ema', ema_udf(df.v1).over(window))
```

## aggregation
#### group aggregation (weighted mean, scalar)
```
import numpy as np
def weighted_mean(vals, weights):
    @pandas_udf(DoubleType())
    def weighted_mean_udf(df):
        return np.average(df[vals], weights=df[weights])
    return weighted_mean_udf

def weighed_mean(df):
    return np.average(df[:,0], weight=df[:,1])

weighted_mean_udf = pandas_udf(weighted_mean, DoubleType())

df.groupBy('id').agg(weighted_mean_udf(df.v1, df.w).as('v1_wm'))
```

## apply
#### partition apply
```
from scipy.stats import mstats
@pandas_udf(DoubleType())
def winsorize_udf(df):
    return mstats.winsorize(df.ix[:,0])

df.papply(winsorize_udf(df.columns))
```
This will apply `winsorize` on each partition and combine results together in the order of partitions.

#### group apply
```
from scipy.stats import mstats

# This must match the returned pandas DataFrame of my_pandas_fn
schema = StructType([StructField('id', IntegerType()), StructField("v1", DoubleType())])

# TODO: Have a better and more complex example
@pandas_udf(schenma)
def winsorize_udf(df):
    df['v1'] = mstats.winsorize(df['v1'], [0.05, 0.05])
    return df

df.groupBy('id').apply(winsorize_udf('id', 'v1'))
```
This will apply `winsorize` on each group and combine the results together into a pyspark DataFrame.
