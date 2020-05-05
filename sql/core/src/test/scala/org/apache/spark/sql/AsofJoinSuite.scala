/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.sql.Timestamp

import scala.concurrent.duration.Duration

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.NullType

class AsofJoinSuite extends QueryTest with SharedSQLContext {
  import testImplicits._


  val seconds: Long = 1000
  val minutes: Long = 60 * seconds
  val hours: Long = 60 * minutes
  val days: Long = 24 * hours
  val months: Long = 30 * days
  val years: Long = 12 * months

  test("basic") {
    val df1 = Seq(
      (new Timestamp(2001), 1, 1.0),
      (new Timestamp(2001), 2, 1.1),
      (new Timestamp(2002), 1, 1.2)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(2001), 1, 4),
      (new Timestamp(2001), 2, 5)
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.asofJoin(df2, df1("time"), df2("time"), df1("id"), df2("id")),
      Seq(
        Row(new Timestamp(2001), 1, 1.0, 4),
        Row(new Timestamp(2002), 1, 1.2, 4),
        Row(new Timestamp(2001), 2, 1.1, 5)
      ))

    checkAnswer(
      df1.select("time", "id").asofJoin(
        df2.withColumn("v3", df2("v2") * 3 cast "Int"),
        df1("time"),
        df2("time"),
        df1("id"),
        df2("id")),
      Seq(
        Row(new Timestamp(2001), 1, 4, 12),
        Row(new Timestamp(2002), 1, 4, 12),
        Row(new Timestamp(2001), 2, 5, 15)
      ))
  }

  test("default asof join") {
    val quotes = Seq(
      (new Timestamp(23), "GOOG", 720.50, 720.93),
      (new Timestamp(23), "MSFT", 51.95, 51.96),
      (new Timestamp(30), "MSFT", 51.97, 51.98),
      (new Timestamp(41), "MSFT", 51.99, 52.00),
      (new Timestamp(48), "GOOG", 720.50, 720.93),
      (new Timestamp(49), "AAPL", 97.99, 98.01),
      (new Timestamp(72), "GOOG", 720.50, 720.88),
      (new Timestamp(75), "MSFT", 52.01, 52.03)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), "MSFT", 51.95, 75),
      (new Timestamp(38), "MSFT", 51.95, 155),
      (new Timestamp(48), "GOOG", 720.77, 100),
      (new Timestamp(48), "GOOG", 720.92, 100),
      (new Timestamp(48), "AAPL", 98.00, 100)
    ).toDF("time", "ticker", "price", "quantity")

    checkAnswer(
      trades.asofJoin(quotes, trades("time"), quotes("time"), trades("ticker"), quotes("ticker")),
      Seq(
        Row(new Timestamp(23), "MSFT", 51.95, 75, 51.95, 51.96),
        Row(new Timestamp(38), "MSFT", 51.95, 155, 51.97, 51.98),
        Row(new Timestamp(48), "GOOG", 720.77, 100, 720.5, 720.93),
        Row(new Timestamp(48), "GOOG", 720.92, 100, 720.5, 720.93),
        Row(new Timestamp(48), "AAPL", 98.0, 100, null, null)
      ))
  }

  test("partial key mismatch") {
    val df1 = Seq(
      (new Timestamp(2001), 1, 1.0),
      (new Timestamp(2001), 2, 1.1),
      (new Timestamp(2002), 1, 1.2)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(2001), 1, 5),
      (new Timestamp(2001), 4, 4)
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.asofJoin(df2, df1("time"), df2("time"), df1("id"), df2("id")),
      Seq(
        Row(new Timestamp(2001), 1, 1.0, 5),
        Row(new Timestamp(2002), 1, 1.2, 5),
        Row(new Timestamp(2001), 2, 1.1, null)
      ))
  }

  test("complete key mismatch") {
    val df1 = Seq(
      (new Timestamp(2001), 1, 1.0),
      (new Timestamp(2001), 2, 1.1),
      (new Timestamp(2002), 1, 1.2)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(2001), 3, 5),
      (new Timestamp(2001), 4, 4)
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.asofJoin(df2, df1("time"), df2("time"), df1("id"), df2("id")),
      Seq(
        Row(new Timestamp(2001), 1, 1.0, null),
        Row(new Timestamp(2002), 1, 1.2, null),
        Row(new Timestamp(2001), 2, 1.1, null)
      ))
  }

  test("asof join tolerance") {
    val quotes = Seq(
      (new Timestamp(23), "GOOG", 720.50, 720.93),
      (new Timestamp(23), "MSFT", 51.95, 51.96),
      (new Timestamp(30), "MSFT", 51.97, 51.98),
      (new Timestamp(41), "MSFT", 51.99, 52.00),
      (new Timestamp(48), "GOOG", 720.50, 720.93),
      (new Timestamp(49), "AAPL", 97.99, 98.01),
      (new Timestamp(72), "GOOG", 720.50, 720.88),
      (new Timestamp(75), "MSFT", 52.01, 52.03)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), "MSFT", 51.95, 75),
      (new Timestamp(38), "MSFT", 51.95, 155),
      (new Timestamp(48), "GOOG", 720.77, 100),
      (new Timestamp(48), "GOOG", 720.92, 100),
      (new Timestamp(48), "AAPL", 98.00, 100)
    ).toDF("time", "ticker", "price", "quantity")

    checkAnswer(
      trades.asofJoin(
        quotes,
        trades("time"),
        quotes("time"),
        trades("ticker"),
        quotes("ticker"),
        "2ms"),
      Seq(
        Row(new Timestamp(23), "MSFT", 51.95, 75, 51.95, 51.96),
        Row(new Timestamp(38), "MSFT", 51.95, 155, null, null),
        Row(new Timestamp(48), "GOOG", 720.77, 100, 720.5, 720.93),
        Row(new Timestamp(48), "GOOG", 720.92, 100, 720.5, 720.93),
        Row(new Timestamp(48), "AAPL", 98.0, 100, null, null)
      ))
  }

  test("asof join tolerance allow exact matches") {
    val quotes = Seq(
      (new Timestamp(23), "GOOG", 720.50, 720.93),
      (new Timestamp(23), "MSFT", 51.95, 51.96),
      (new Timestamp(30), "MSFT", 51.97, 51.98),
      (new Timestamp(41), "MSFT", 51.99, 52.00),
      (new Timestamp(48), "GOOG", 720.50, 720.93),
      (new Timestamp(49), "AAPL", 97.99, 98.01),
      (new Timestamp(72), "GOOG", 720.50, 720.88),
      (new Timestamp(75), "MSFT", 52.01, 52.03)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), "MSFT", 51.95, 75),
      (new Timestamp(38), "MSFT", 51.95, 155),
      (new Timestamp(48), "GOOG", 720.77, 100),
      (new Timestamp(48), "GOOG", 720.92, 100),
      (new Timestamp(48), "AAPL", 98.00, 100)
    ).toDF("time", "ticker", "price", "quantity")

    checkAnswer(
      trades.asofJoin(
        quotes,
        trades("time"),
        quotes("time"),
        trades("ticker"),
        quotes("ticker"),
        "10ms",
        false),
      Seq(
        Row(new Timestamp(23), "MSFT", 51.95, 75, null, null),
        Row(new Timestamp(38), "MSFT", 51.95, 155, 51.97, 51.98),
        Row(new Timestamp(48), "GOOG", 720.77, 100, null, null),
        Row(new Timestamp(48), "GOOG", 720.92, 100, null, null),
        Row(new Timestamp(48), "AAPL", 98.0, 100, null, null)
      ))
  }

  test("asof join with non timestamp types") {
    val quotes = Seq(
      (1, "GOOG", 720.50, 720.93),
      (2, "MSFT", 51.95, 51.96),
      (3, "MSFT", 51.97, 51.98),
      (4, "MSFT", 51.99, 52.00),
      (5, "GOOG", 720.50, 720.93),
      (6, "AAPL", 97.99, 98.01),
      (7, "GOOG", 720.50, 720.88),
      (8, "MSFT", 52.01, 52.03)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), "MSFT", 51.95, 75),
      (new Timestamp(38), "MSFT", 51.95, 155),
      (new Timestamp(48), "GOOG", 720.77, 100),
      (new Timestamp(48), "GOOG", 720.92, 100),
      (new Timestamp(48), "AAPL", 98.00, 100)
    ).toDF("time", "ticker", "price", "quantity")

    intercept[AnalysisException](trades.asofJoin(
      quotes,
      trades("time"),
      quotes("time"),
      trades("ticker"),
      quotes("ticker")))
    intercept[AnalysisException](quotes.asofJoin(
      trades,
      quotes("time"),
      trades("time"),
      quotes("ticker"),
      trades("ticker")))
  }

  test("asof join with different types in 'by' key") {
    val quotes = Seq(
      (new Timestamp(23), 1, 720.50, 720.93),
      (new Timestamp(23), 2, 51.95, 51.96),
      (new Timestamp(48), 2, 51.97, 51.98),
      (new Timestamp(48), 2, 51.99, 52.00)
    ).toDF("time", "ticker", "bid", "ask")

    val trades = Seq(
      (new Timestamp(23), 2L, 51.95, 75),
      (new Timestamp(38), 2L, 51.95, 155),
      (new Timestamp(48), 1L, 720.77, 100),
      (new Timestamp(48), 1L, 720.92, 100)
    ).toDF("time", "ticker", "price", "quantity")

    intercept[AnalysisException](trades.asofJoin(
      quotes,
      trades("time"),
      quotes("time"),
      trades("ticker"),
      quotes("ticker")))
    intercept[AnalysisException](quotes.asofJoin(
      trades,
      quotes("time"),
      trades("time"),
      quotes("ticker"),
      trades("ticker")))
  }

  test("self asof join on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    checkAnswer(
      df.asofJoin(df, df("time"), df("time"), df("id"), df("id")),
      Seq(
        Row(new Timestamp(100), 1, "a", "a"),
        Row(new Timestamp(117), 1, "b", "b"),
        Row(new Timestamp(118), 1, "c", "c"),
        Row(new Timestamp(119), 1, "d", "d"),
        Row(new Timestamp(101), 2, "a", "a"),
        Row(new Timestamp(102), 3, "a", "a"),
        Row(new Timestamp(103), 4, "a", "a"),
        Row(new Timestamp(104), 5, "a", "a"),
        Row(new Timestamp(105), 6, "a", "a"),
        Row(new Timestamp(120), 6, "b", "b"),
        Row(new Timestamp(121), 6, "c", "c"),
        Row(new Timestamp(106), 7, "a", "a"),
        Row(new Timestamp(107), 8, "a", "a"),
        Row(new Timestamp(108), 9, "a", "a"),
        Row(new Timestamp(109), 10, "a", "a"),
        Row(new Timestamp(110), 11, "a", "a"),
        Row(new Timestamp(122), 11, "b", "b"),
        Row(new Timestamp(111), 12, "a", "a"),
        Row(new Timestamp(112), 13, "a", "a"),
        Row(new Timestamp(113), 14, "a", "a"),
        Row(new Timestamp(114), 15, "a", "a"),
        Row(new Timestamp(115), 16, "a", "a"),
        Row(new Timestamp(116), 17, "a", "a")
      ))
  }

  test("asof join on left superset on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    val df1 = Seq(
      (new Timestamp(100), 1, "b"),
      (new Timestamp(101), 3, "b"),
      (new Timestamp(102), 5, "b"),
      (new Timestamp(103), 7, "b"),
      (new Timestamp(104), 9, "b"),
      (new Timestamp(105), 11, "b")
    ).toDF("time", "id", "v2")

    checkAnswer(
      df.asofJoin(df1, df("time"), df1("time"), df("id"), df1("id")),
      Seq(
        Row(new Timestamp(100), 1, "a", "b"),
        Row(new Timestamp(117), 1, "b", "b"),
        Row(new Timestamp(118), 1, "c", "b"),
        Row(new Timestamp(119), 1, "d", "b"),
        Row(new Timestamp(101), 2, "a", null),
        Row(new Timestamp(102), 3, "a", "b"),
        Row(new Timestamp(103), 4, "a", null),
        Row(new Timestamp(104), 5, "a", "b"),
        Row(new Timestamp(105), 6, "a", null),
        Row(new Timestamp(120), 6, "b", null),
        Row(new Timestamp(121), 6, "c", null),
        Row(new Timestamp(106), 7, "a", "b"),
        Row(new Timestamp(107), 8, "a", null),
        Row(new Timestamp(108), 9, "a", "b"),
        Row(new Timestamp(109), 10, "a", null),
        Row(new Timestamp(110), 11, "a", "b"),
        Row(new Timestamp(122), 11, "b", "b"),
        Row(new Timestamp(111), 12, "a", null),
        Row(new Timestamp(112), 13, "a", null),
        Row(new Timestamp(113), 14, "a", null),
        Row(new Timestamp(114), 15, "a", null),
        Row(new Timestamp(115), 16, "a", null),
        Row(new Timestamp(116), 17, "a", null)
      ))
  }

  test("asof join on right superset on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    val df1 = Seq(
      (new Timestamp(100), 1, "b"),
      (new Timestamp(101), 3, "b"),
      (new Timestamp(102), 5, "b"),
      (new Timestamp(103), 7, "b"),
      (new Timestamp(104), 9, "b"),
      (new Timestamp(105), 11, "b")
    ).toDF("time", "id", "v2")

    checkAnswer(
      df1.asofJoin(df, df1("time"), df("time"), df1("id"), df("id")),
      Seq(
        Row(new Timestamp(100), 1, "b", "a"),
        Row(new Timestamp(101), 3, "b", null),
        Row(new Timestamp(102), 5, "b", null),
        Row(new Timestamp(103), 7, "b", null),
        Row(new Timestamp(104), 9, "b", null),
        Row(new Timestamp(105), 11, "b", null)
      ))
  }

  test("asof join on left intersect on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(114), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(116), 17, "a")
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(100), 15, "c"),
      (new Timestamp(101), 16, "c"),
      (new Timestamp(102), 17, "c"),
      (new Timestamp(103), 18, "c"),
      (new Timestamp(104), 19, "c"),
      (new Timestamp(105), 20, "c")
    ).toDF("time", "id", "v3")

    checkAnswer(
      df.asofJoin(df2, df("time"), df2("time"), df("id"), df2("id")),
      Seq(
        Row(new Timestamp(100), 1, "a", null),
        Row(new Timestamp(117), 1, "b", null),
        Row(new Timestamp(118), 1, "c", null),
        Row(new Timestamp(119), 1, "d", null),
        Row(new Timestamp(101), 2, "a", null),
        Row(new Timestamp(102), 3, "a", null),
        Row(new Timestamp(103), 4, "a", null),
        Row(new Timestamp(104), 5, "a", null),
        Row(new Timestamp(105), 6, "a", null),
        Row(new Timestamp(120), 6, "b", null),
        Row(new Timestamp(121), 6, "c", null),
        Row(new Timestamp(106), 7, "a", null),
        Row(new Timestamp(107), 8, "a", null),
        Row(new Timestamp(108), 9, "a", null),
        Row(new Timestamp(109), 10, "a", null),
        Row(new Timestamp(110), 11, "a", null),
        Row(new Timestamp(122), 11, "b", null),
        Row(new Timestamp(111), 12, "a", null),
        Row(new Timestamp(112), 13, "a", null),
        Row(new Timestamp(113), 14, "a", null),
        Row(new Timestamp(114), 15, "a", "c"),
        Row(new Timestamp(115), 16, "a", "c"),
        Row(new Timestamp(116), 17, "a", "c")
      ))
  }

  test("asof join on right intersect on larger dataset") {
    val df = Seq(
      (new Timestamp(100), 1, "a"),
      (new Timestamp(117), 1, "b"),
      (new Timestamp(118), 1, "c"),
      (new Timestamp(119), 1, "d"),
      (new Timestamp(101), 2, "a"),
      (new Timestamp(102), 3, "a"),
      (new Timestamp(103), 4, "a"),
      (new Timestamp(104), 5, "a"),
      (new Timestamp(105), 6, "a"),
      (new Timestamp(120), 6, "b"),
      (new Timestamp(121), 6, "c"),
      (new Timestamp(106), 7, "a"),
      (new Timestamp(107), 8, "a"),
      (new Timestamp(108), 9, "a"),
      (new Timestamp(109), 10, "a"),
      (new Timestamp(110), 11, "a"),
      (new Timestamp(122), 11, "b"),
      (new Timestamp(111), 12, "a"),
      (new Timestamp(112), 13, "a"),
      (new Timestamp(113), 14, "a"),
      (new Timestamp(99), 15, "a"),
      (new Timestamp(115), 16, "a"),
      (new Timestamp(100), 17, "a")
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(100), 15, "c"),
      (new Timestamp(101), 16, "c"),
      (new Timestamp(102), 17, "c"),
      (new Timestamp(103), 18, "c"),
      (new Timestamp(104), 19, "c"),
      (new Timestamp(105), 20, "c")
    ).toDF("time", "id", "v3")

    checkAnswer(
      df2.asofJoin(df, df2("time"), df("time"), df2("id"), df("id")),
      Seq(
        Row(new Timestamp(100), 15, "c", "a"),
        Row(new Timestamp(101), 16, "c", null),
        Row(new Timestamp(102), 17, "c", "a"),
        Row(new Timestamp(103), 18, "c", null),
        Row(new Timestamp(104), 19, "c", null),
        Row(new Timestamp(105), 20, "c", null)
      ))
  }


  test("broadcast asof join") {
    val df = Seq(
      (new Timestamp(100), 1, 1.0),
      (new Timestamp(100), 1, 2.0),
      (new Timestamp(100), 2, 3.0),
      (new Timestamp(110), 1, 4.0),
      (new Timestamp(110), 2, 5.0)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(100), 10.0),
      (new Timestamp(105), 20.0),
      (new Timestamp(115), 15.0)
    ).toDF("time", "v2")


    val result = df.asofJoin(df2, df("time"), df2("time"), lit(null), lit(null))

    checkAnswer(
      result,
      Seq(
        Row(new Timestamp(100), 1, 1.0, 10.0),
        Row(new Timestamp(100), 1, 2.0, 10.0),
        Row(new Timestamp(100), 2, 3.0, 10.0),
        Row(new Timestamp(110), 1, 4.0, 20.0),
        Row(new Timestamp(110), 2, 5.0, 20.0)
      ))
  }

  test("broadcast asof join - right unsorted") {
    val df = Seq(
      (new Timestamp(100), 1, 1.0),
      (new Timestamp(100), 1, 2.0),
      (new Timestamp(100), 2, 3.0),
      (new Timestamp(110), 1, 4.0),
      (new Timestamp(110), 2, 5.0)
    ).toDF("time", "id", "v")

    val df2 = Seq(
      (new Timestamp(100), new Timestamp(105), 20.0),
      (new Timestamp(105), new Timestamp(100), 10.0),
      (new Timestamp(115), new Timestamp(115), 15.0)
    ).toDF("other_time", "time", "v2")

    val result = df.asofJoin(df2, df("time"), df2("time"), lit(null), lit(null))

    checkAnswer(
      result,
      Seq(
        Row(new Timestamp(100), 1, 1.0, new Timestamp(105), 10.0),
        Row(new Timestamp(100), 1, 2.0, new Timestamp(105), 10.0),
        Row(new Timestamp(100), 2, 3.0, new Timestamp(105), 10.0),
        Row(new Timestamp(110), 1, 4.0, new Timestamp(100), 20.0),
        Row(new Timestamp(110), 2, 5.0, new Timestamp(100), 20.0)
      ))
  }

  test("generated intervalized test - broadcast") {
    val lData = genIntervalizedData(
      freq = "15min",
      begin = new Timestamp(50*years),
      end = new Timestamp(50*years + 1*months),
      beginHour = 9,
      endHour = 17,
      keys = 50,
      values = 1,
      123,
      0.9
    )

    val rData = genIntervalizedData(
      freq = "15min",
      begin = new Timestamp(50*years),
      end = new Timestamp(50*years + 1*months),
      beginHour = 9,
      endHour = 17,
      keys = 1,
      values = 1,
      456,
      0.9
    ).drop("id")

    compare(
      lData,
      rData,
      lData("time"),
      rData("time"),
      lit(null),
      lit(null),
      Long.MaxValue,
      true)
  }

  test("generated intervalized test - dense") {
    val lData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + 1*months),
      9,
      17,
      50,
      1,
      123,
      0.9
    )

    val rData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + 1*months),
      9,
      17,
      100,
      1,
      456,
      0.9
    )

    compare(
      lData,
      rData,
      lData("time"),
      rData("time"),
      lData("id"),
      rData("id"),
      Long.MaxValue,
      true)
  }

  test("generated intervalized test - sparse") {
    val lData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + 1*months),
      9,
      17,
      50,
      1,
      234,
      0.9
    )

    val rData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + 1*months),
      9,
      17,
      100,
      1,
      345,
      0.1
    )

    compare(
      lData,
      rData,
      lData("time"),
      rData("time"),
      lData("id"),
      rData("id"),
      Long.MaxValue,
      true)
  }

  test("generated intervalized test - dense, high tolerance") {
    val lData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + months),
      9,
      17,
      50,
      1,
      456,
      0.9
    )

    val rData = genIntervalizedData(
      "30min",
      new Timestamp(50*years),
      new Timestamp(50*years + months),
      9,
      17,
      100,
      1,
      567,
      0.9
    )

    compare(
      lData,
      rData,
      lData("time"),
      rData("time"),
      lData("id"),
      rData("id"),
      6 * hours.toLong,
      true)
  }

  test("generated intervalized test - sparse, low tolerance") {
    val lData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + months),
      9,
      17,
      50,
      1,
      567,
      0.1
    )

    val rData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + 1*months),
      9,
      17,
      100,
      1,
      789,
      0.9
    )

    compare(
      lData,
      rData,
      lData("time"),
      rData("time"),
      lData("id"),
      rData("id"),
      hours.toLong,
      true)
  }

  test("generated intervalized test - dense, high tolerance, inexact matching") {
    val lData = genIntervalizedData(
      "15min",
      new Timestamp(50*years),
      new Timestamp(50*years + 1*months),
      9,
      17,
      50,
      1,
      678,
      0.9
    )

    val rData = genIntervalizedData(
      "30min",
      new Timestamp(50*years),
      new Timestamp(50*years + 1*months),
      9,
      17,
      100,
      1,
      999,
      0.9
    )

    compare(
      lData,
      rData,
      lData("time"),
      rData("time"),
      lData("id"),
      rData("id"),
      6 * hours.toLong,
      false)
  }

  def compare(
    lData: DataFrame,
    rData: DataFrame,
    leftOn: Column,
    rightOn: Column,
    leftBy: Column,
    rightBy: Column,
    tolerance: Long = Long.MaxValue,
    exactMatches: Boolean = true
  ): Unit = {
    val col = {
      lData.columns.toList ++
        rData.columns.map(c => "r" + c).filter(c => {c != "rtime" && c != "rid"}).toList
    }

    var res = lData
    var expected = lData
    if (tolerance == Long.MaxValue) {
      res = lData.asofJoin(
        rData,
        leftOn,
        rightOn,
        leftBy,
        rightBy,
        "Inf",
        exactMatches).toDF(col: _*)
      expected = naive(lData, rData, leftOn, rightOn, leftBy, rightBy, tolerance, exactMatches)
    } else {
      res = lData.asofJoin(
        rData,
        leftOn,
        rightOn,
        leftBy,
        rightBy,
        tolerance.toString + "ms",
        exactMatches).toDF(col: _*)
      expected = naive(
        lData,
        rData,
        leftOn,
        rightOn,
        leftBy,
        rightBy,
        tolerance / 1000,
        exactMatches)
    }
    checkAnswer(res, expected)
  }

  private def genIntervalizedData(
    freq: String,
    begin: Timestamp,
    end: Timestamp,
    beginHour: Int,
    endHour: Int,
    keys: Int,
    values: Int,
    seed: Long,
    bias: Double
  ): DataFrame = {
    val frequency = Duration(freq).toSeconds
    val delta = (end.getTime-begin.getTime) / 1000
    val dates = for (i <- spark.range(0, delta / frequency))
      yield {begin.getTime / 1000 + i * frequency}
    var df = dates.toDF("time")
    df = df.withColumn("ids", functions.array((0 until keys).map(functions.lit): _*))
    df = df.withColumn("id", functions.explode(df.col("ids"))).drop("ids")
    for (i <- 1 to values) {
      df = df.withColumn(s"v$i", functions.rand(seed = seed) - 0.5)
    }
    df = df.withColumn("time", functions.col("time").cast("timestamp"))
    df = df.filter(functions.hour(df.col("time")) >= beginHour)
      .filter(functions.hour(df.col("time")) < endHour)
    df = df.filter(functions.rand(seed = seed) <= bias)

    df
  }

  private def naive(
    left: DataFrame,
    right: DataFrame,
    leftOn: Column,
    rightOn: Column,
    leftBy: Column,
    rightBy: Column,
    tolerance: Long = Long.MaxValue,
    exactMatches: Boolean): DataFrame = {

    val rCol = right.columns.map(c => "r" + c).toList
    val newRight = right.toDF(rCol: _*)

    val newLeft = if (leftBy.expr.dataType == NullType) {
      left.withColumn("tid", lit(0))
    } else {
      left
    }

    val res = if (leftBy.expr.dataType != NullType) {
      (tolerance, exactMatches) match {
        case (Long.MaxValue, true) => left.join(
          newRight,
          left("id") === newRight("rid") && left("time") >= newRight("rtime"),
          "left_outer"
        )
        case (Long.MaxValue, false) => left.join(
          newRight,
          left("id") === newRight("rid") && left("time") > newRight("rtime"),
          "left_outer"
        )
        case (_, true) => left.join(
          newRight,
          left("id") === newRight("rid") && left("time") >= newRight("rtime") &&
            left("time").cast("long") <= newRight("rtime").cast("long") + tolerance,
          "left_outer"
        )
        case (_, false) => left.join(
          newRight,
          left("id") === newRight("rid") && left("time") > newRight("rtime") &&
            left("time").cast("long") <= newRight("rtime").cast("long") + tolerance,
          "left_outer"
        )
      }
    } else {
      (tolerance, exactMatches) match {
        case (Long.MaxValue, true) => left.join(
          newRight,
          left("time") >= newRight("rtime"),
          "left_outer"
        )
        case (Long.MaxValue, false) => left.join(
          newRight,
          left("time") > newRight("rtime"),
          "left_outer"
        )
        case (_, true) => left.join(
          newRight,
          left("time") >= newRight("rtime") &&
            left("time").cast("long") <= newRight("rtime").cast("long") + tolerance,
          "left_outer"
        )
        case (_, false) => left.join(
          newRight,
          left("time") > newRight("rtime") &&
            left("time").cast("long") <= newRight("rtime").cast("long") + tolerance,
          "left_outer"
        )
      }
    }

    var maxTime = res.groupBy("time", "id").agg(functions.max(newRight("rtime"))).sort("id")
    maxTime = maxTime.toDF("time", "id", "maxtime")

    var res2 = left.join(maxTime, Seq("time", "id"), joinType = "left")
    if (leftBy.expr.dataType != NullType) {
      res2 = res2.join(
        right,
        res2("id") === right("id") && res2("maxtime") === right("time"),
        joinType = "left"
      )
    } else {
      res2 = res2.join(
        right,
        res2("maxtime") === right("time"),
        joinType = "left"
      )
    }
    val resCol = left.columns.toList ++ Seq("maxtime") ++ rCol

    res2.toDF(resCol: _*).drop("maxtime").drop("rtime").drop("rid")
  }
}
