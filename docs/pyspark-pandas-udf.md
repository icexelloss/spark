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
# Definition of pandas udf
We introduce a `pandas_udf` decorator that allows to define a user defined function that operates on pandas data structure such as `pandas.Series` and `pandas.DataFrame`.

There are three parts to a udf: the input, the output and the function. To illustrate this, consider the following example:

```
@pandas_udf(SeriesType(DoubleType()))
def foo(v, w):
    return v * w

df = ...
udf_column = foo(df.v, df.w)
```

This defines a udf that takes two input column: `df.v` and `df.w` and multiply them. The input to `def foo(v, w)` are pandas Series, the output `v * w` is also a pandas Series.

## Input
The input is defined in `foo(df.v, df.w)`. This defines the input to the function: column `df.v` and column `df.w`. As the result, these two columns will be converted into two pandas Series and pass into the function `def foo(v, w)`. Natually, the defition `def foo(v, w)` must match the invocation `foo(df.v, df.w)`, otherwise an exception will be thrown to the user.

In addition to `pandas.Series`, we also want to support function that takes `pandas.DataFrame` as input, this is useful when the function needs to operate on many columns, as illustrated below:

```
@pandas_udf(SeriesType(DoubleType()))
def bar(df):
    return df.v1 + df.v2 + df.v3 + df.v4 + df.v5
udf_column = bar(df[['v1', 'v2', 'v3', 'v4', 'v5']])
```

## Output
The output is defined in `@pandas_udf(SeriesType(DoubleType()))`. This defines the output of the function to be a `pandas.Series` of doubles. This must match the return value, i.e. `return v * w`, otherwise an exception will be thrown to the user.

In addition to `pandas.Series`, we also want to support function that returns a `scalar` value as well as a `pandas.DataFrame`, as illustrated below:

Scalar:
```
@pandas_udf(DoubleType())
def foo(v, w):
    return np.average(v, w)
```
pandas.DataFrame:
```
@pandas_udf(DataFrameType([StructField('v1', DoubleType()), StructField('v2', DoubleType())]))
def foo(v, w):
    v1 = v + 1
    v2 = w * v1
    return pd.DataFrame([v1, v2])
```

# Use of pandas udf
## Vectorized row operations
In this example the udf takes one or more `pandas.Series` of the same size, returns a `pandas.Series` of the same size. The returned `pandas.Series` it appeded to each row. How data in spark is grouped into pandas.Series is implementation detail and the user should not reply on it.
```
@pandas_udf(SeriesType(DoubleType()))
def plus(v1, v2):
    return v1 + v2

df.withColumn('sum', plus(df.v1, df.v2))
```
input

| id        | v1           | v2           |
| --------- | ------------ | ------------ |
| foo       | 1.0          | 1            |
| bar       | 2.0          | 2            |
| foo       | 3.0          | 1            |
| foo       | 4.0          | 3            |
| bar       | 5.0          | 2            |
| foo       | 6.0          | 1            |

output

| id        | v1           | v2           | sum         |
| --------- | ------------ | ------------ | ------------|
| foo       | 1.0          | 1            | 2.0         |
| bar       | 2.0          | 2            | 4.0         |
| foo       | 3.0          | 1            | 4.0         |
| foo       | 4.0          | 3            | 7.0         |
| bar       | 5.0          | 2            | 7.0         |
| foo       | 6.0          | 1            | 7.0         |

## Window Operations
### Non overlapping windows
Non overlapping window is conceptually same as `groupBy`. So alternatively we can also use a `groupBy` operator.

#### Non overlapping windows (Series)
In this example, the udf takes one or more `pandas.Series` of the same size, and returns a `pandas.Series` of the same size. The returned Series is joined with rows of the window.

```
w = Window.partitionBy('id')

@pandas_udf(SeriesType(DoubleType()))
def rank_udf(v):
    return v.rank(pct=True)

df.withColumn('rank', rank_udf(df.v).over(w))
```
input

| id        | v1            |
| --------- | ------------- |
| foo       | 1.0           |
| bar       | 2.0           |
| foo       | 3.0           |
| foo       | 4.0           |
| bar       | 5.0           |
| foo       | 6.0           |

output

| id        | v            | rank         |
| --------- | ------------ | ------------ |
| foo       | 1.0          | 0.25         |
| bar       | 2.0          | 0.5          |
| foo       | 3.0          | 0.5          |
| foo       | 4.0          | 0.75         |
| bar       | 5.0          | 1.0          |
| foo       | 6.0          | 1.0          |

#### None overlapping windows (Scalar)
In this example, the udf takes one or more pd.Series of the same size, and returns a scalar value. The return value is appended to each row of the window.

```
import numpy as np

w = Window.partitionBy('id')

@pandas_udf(DoubleType())
def weighted_mean_udf(v1, w):
    return np.average(v1, weights=w)

df.withColumn('v1_vm', weighted_mean_udf(df.v1, df.w).over(w))
```

input

| id        | v1           | w            |
| --------- | ------------ | ------------ |
| foo       | 1.0          | 1            |
| bar       | 2.0          | 2            |
| foo       | 3.0          | 1            |
| foo       | 4.0          | 3            |
| bar       | 5.0          | 2            |
| foo       | 6.0          | 1            |

output

| id        | v1           | w            | v1_vm            |
| --------- | ------------ | ------------ | ---------------- |
| foo       | 1.0          | 1            | 3.67             |
| bar       | 2.0          | 2            | 3.5              |
| foo       | 3.0          | 1            | 3.67             |
| foo       | 4.0          | 3            | 3.67             |
| bar       | 5.0          | 2            | 3.5              |
| foo       | 6.0          | 1            | 3.67             |

### Overlapping windows (Rolling windows)
Overlapping windows is defined by using `rangeBetween` or `rowsBetween`. With overlapping windows, each row in the original table has a different window.

#### Overlapping windows (Scalar)
In this example, the udf takes one or more `pandas.Series` of the same size, and returns a scalar value. The return value is added to each row of the window.
```

w = Window.partitionBy('id').orderBy('time').rangeBetween(-200, 0)

@pandas_udf(DoubleType())
def ema_udf(v1):
    return v1.ewm(alpha=0.5).mean().iloc[-1]

df.withColumn('v1_ema', ema_udf(df.v1).over(window))
```
input

| time      | id        | v1           |
| --------- | --------- | ------------ |
| 100       | foo       | 1.0          |
| 100       | bar       | 2.0          |
| 200       | foo       | 3.0          |
| 200       | foo       | 4.0          |
| 200       | bar       | 5.0          |
| 300       | foo       | 6.0          |

output

|time       | id        | v1           | v1_ema        |
| --------- | --------- | ------------ | ------------- |
| 100       | foo       | 1.0          | 1.0           |
| 100       | bar       | 2.0          | 2.0           |
| 200       | foo       | 3.0          | 2.33          |
| 200       | foo       | 4.0          | 3.28          |
| 200       | bar       | 5.0          | 4.0           |
| 300       | foo       | 6.0          | 4.73          |

## Group Operations
#### aggregation
In this example, the udf takes one or more `pandas.Series` of the same size, and returns a scalar value. The result is the aggregation for each group.

```
import numpy as np
@pandas_udf(DoubleType())
def weighted_mean_udf(v1, w):
    return np.average(v1, weights=w)

df.groupBy('id').agg(weighted_mean_udf(df.v1, df.w).as('v1_wm'))
```

input

| id        | v1           | w            |
| --------- | ------------ | ------------ |
| foo       | 1.0          | 1            |
| bar       | 2.0          | 2            |
| foo       | 3.0          | 1            |
| foo       | 4.0          | 3            |
| bar       | 5.0          | 2            |
| foo       | 6.0          | 1            |

output

| id        | v1_wm        |
| --------- | ------------ |
| foo       | 3.67         |
| bar       | 3.5          |

#### transform
```
@pandas_udf(SeriesType(DoubleType()))
def normalize(v):
    v = (v - v.mean()) / v.std()
    return v

df.groupBy('id').transform(normalize(df.v1))
```

Note: This is equivalent to the following example using window:
```
w = Window.partitionBy('id')
df.withColumn('v1', normalize(df.v1).over(w))
```

input

| time      | id        | v1           |
| --------- | --------- | ------------ |
| 100       | foo       | 1.0          |
| 100       | bar       | 2.0          |
| 200       | foo       | 3.0          |
| 200       | foo       | 4.0          |
| 200       | bar       | 5.0          |
| 300       | foo       | 6.0          |

output

| time      | id        | v1           |
| --------- | --------- | ------------ |
| 100       | foo       | -1.2         |
| 100       | bar       | -0.7         |
| 200       | foo       | -0.24        |
| 200       | foo       | 0.24         |
| 200       | bar       | 0.7          |
| 300       | foo       | 1.2          |

#### apply
In this example, the udf takes a `pandas.DataFrame` and returns a `pandas.DataFrame`. Here the input and output dataframe has the same schema and size, but they don't have too. (Input and output can be different in both schema and size)
```
schema = df.schema

@pandas_udf(schema)
def normalize(df):
    df.v1 = (df.v1 - df.v1.mean()) / df.v1.std()
    return df

df.groupBy('id').apply(normalize(df))
```
This will apply `normalize` on each group and combine the results together into a pyspark DataFrame.

input

| time      | id        | v1           |
| --------- | --------- | ------------ |
| 100       | foo       | 1.0          |
| 100       | bar       | 2.0          |
| 200       | foo       | 3.0          |
| 200       | foo       | 4.0          |
| 200       | bar       | 5.0          |
| 300       | foo       | 6.0          |

output

| time      | id        | v1           |
| --------- | --------- | ------------ |
| 100       | foo       | -1.2         |
| 100       | bar       | -0.7         |
| 200       | foo       | -0.24        |
| 200       | foo       | 0.24         |
| 200       | bar       | 0.7          |
| 300       | foo       | 1.2          |

Note: This example shows a transformation on a column, which can be done using `transform`, but `apply` is more flexiable in general.
