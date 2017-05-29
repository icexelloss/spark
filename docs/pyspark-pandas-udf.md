Pyspark Pandas Udf
==================
split-apply-merge is a useful pattern when analyzing data. It is implemented in many popular data analying libraries such as Spark, Pandas, R, and etc. Split and merge operations in these libraries are similar to each other, mostly implemented by a `group by` operator. For instance, Spark DataFrame has `groupBy`, Pandas DataFrame also has `groupby`. Therefore, for users familiar with either Spark DataFrame or pandas DataFrame, it is not difficult for them to understand how grouping works in the other library. However, `apply` is more native to different libraries and therefore, quite different between libraries. A pandas user knows how to normalize (subtract by mean and divide by std) grouped data might not know how to do the same calculation using pyspark, for instance. Also, the current implementation of passing data from the java executor to python executor is not effiecient, there is oppurtunity to speed it up using Apache Arrow. We are proposing new functions that allows easy and high performance split-apply-merge computation using pyspark and pandas.

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
#### group withColumn (ranking, vector)
In this example, the udf takes one or more pd.Series of the same size as input, and returns a pd.Series of the same size. The returned pd.Series is appended to each row of the window.

```
w = Window.partitionBy('id')

@pandas_udf(Series, DoubleType())
def rank_udf(v):
    return v.rank(pct=True)

df.withColumn('rank', rank_udf(df.v).over(w))
```
input
| id        | v            |
| ----------|:------------:|
| foo       | 1.0          |
| bar       | 2.0          |
| foo       | 3.0          |
| foo       | 4.0          |
| bar       | 5.0          |
| foo       | 6.0          |

output
| id        | v            |rank          |
| ----------|:------------:|:------------:|
| foo       | 1.0          |0.25          |
| bar       | 2.0          |0.5           |
| foo       | 3.0          |0.5           |
| foo       | 4.0          |0.75          |
| bar       | 5.0          |1.0           |
| foo       | 6.0          |1.0           |

#### group withColumn (weighted mean, scalar)
In this example, the udf takes one or more pd.Series of the same size as input, and returns a scalar value.  This returned value is appended to each row of the window.

```
import numpy as np

w = Window.partitionBy('id')

@pandas_udf(Scalar, DoubleType())
def weighted_mean_udf(v1, w):
    return np.average(v1, weights=w)

df.withColumn('v1_vm', weighted_mean_udf(df.v1, df.w).over(w))
```
input
| id        | v1           | w            |
| ----------|:------------:|:------------:|
| foo       | 1.0          | 1            |
| bar       | 2.0          | 2            |
| foo       | 3.0          | 1            |
| foo       | 4.0          | 3            |
| bar       | 5.0          | 2            |
| foo       | 6.0          | 1            |

output
| id        | v1           | w            | v1_vm            |
| ----------|:------------:|:------------:|:----------------:|
| foo       | 1.0          | 1            |3.67              |
| bar       | 2.0          | 2            |3.5               |
| foo       | 3.0          | 1            |3.67              |
| foo       | 4.0          | 3            |3.67              |
| bar       | 5.0          | 2            |3.5               |
| foo       | 6.0          | 1            |3.67              |

#### window withColumn (ema, scalar)
In this example, the udf takes one or more pd.Series of the same size as input, and returns a scalar value. The return value is added toeach row of the window.
```

w = Window.partitionBy('id').orderBy('time).rangeBetween(-200, 0)

@pandas_udf(Scalar, DoubleType())
def ema_udf(v1):
    return v1.ewm(alpha=0.5).mean().iloc[-1]

df.withColumn('v1_ema', ema_udf(df.v1).over(window))
```
input
|time       | id        | v1           |
|:----------|:----------|:------------:|
|100        | foo       | 1.0          |
|100        | bar       | 2.0          |
|200        | foo       | 3.0          |
|200        | foo       | 4.0          |
|200        | bar       | 5.0          |
|300        | foo       | 6.0          |

output
|time       | id        | v1           | v1_ema        |
|:----------|:----------|:------------:|:-------------:|
|100        | foo       | 1.0          |1.0            |
|100        | bar       | 2.0          |2.0            |
|200        | foo       | 3.0          |2.33           |
|300        | foo       | 4.0          |3.28           |
|200        | bar       | 5.0          |4.0            |
|400        | foo       | 6.0          |4.73           |

## aggregation
#### group aggregation (weighted mean, scalar)
```
import numpy as np
@pandas_udf(Scalar, DoubleType())
def weighted_mean_udf(v1, w):
    return np.average(v1, weights=w)

df.groupBy('id').agg(weighted_mean_udf('v1', 'w').as('v1_wm'))
```
input
| id        | v1           | w            |
| ----------|:------------:|:------------:|
| foo       | 1.0          | 1            |
| bar       | 2.0          | 2            |
| foo       | 3.0          | 1            |
| foo       | 4.0          | 3            |
| bar       | 5.0          | 2            |
| foo       | 6.0          | 1            |

output:
| id        | v1_wm        |
| ----------|:------------:|
| foo       |3.67          |
| bar       |3.5           |

## apply

#### partition apply
```
# This must match the returned pandas DataFrame of the udf
schema = StructType([StructField('id', IntegerType()), StructField("v1", DoubleType())])

from scipy.stats import mstats
@pandas_udf(DataFrame, schema)
def winsorize_udf(df):
    df.v = mstats.winsorize(df.v)
    return df

df.papply(winsorize_udf(df.columns))
```
This will apply `winsorize` on each partition and combine results together in the order of partitions.

#### group apply
```
from scipy.stats import mstats

# This must match the returned pandas DataFrame of the udf
schema = StructType([StructField('id', IntegerType()), StructField("v1", DoubleType())])

@pandas_udf(DataFrame, schema)
def winsorize_udf(df):
    df.v1 = mstats.winsorize(df.v1, [0.05, 0.05])
    return df

df.groupBy('id').apply(winsorize_udf(df.columns))
```
This will apply `winsorize` on each group and combine the results together into a pyspark DataFrame.
