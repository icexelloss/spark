#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.sql import Row
import pandas as pd
from datetime import datetime

class AsofJoinTests(ReusedSQLTestCase):

    def data(self):
        pdf1 = pd.DataFrame({"time": [datetime(2001,1,1), datetime(2001,1,1), datetime(2002,1,1)],
                            "id": [1, 2, 1],
                            "v": [1.0, 1.1, 1.2]}, columns=["time", "id", "v"])
        pdf2 = pd.DataFrame({"time": [datetime(2001,1,1), datetime(2001,1,1)],
                            "id": [1, 2],
                            "v2": [4, 5]}, columns=["time", "id", "v2"])
        return pdf1, pdf2

    def test_invalid_join(self):
        pdf1, pdf2 = self.data()
        df1 = self.spark.createDataFrame(pdf1)
        df2 = self.spark.createDataFrame(pdf2)
        self.assertRaises(TypeError, lambda: df1.asofJoin(df2, df1["time"], df2["id"]).count())

    def test_join_with_on_by(self):
        pdf1, pdf2 = self.data()
        df1 = self.spark.createDataFrame(pdf1)
        df2 = self.spark.createDataFrame(pdf2)
        actual = df1.asofJoin(df2, df1["time"], df2["time"], df1["id"],
            df2["id"]).toPandas().sort_values(by='time').reset_index(drop=True)
        self.assertTrue(actual.equals(pd.merge_asof(pdf1, pdf2, on="time", by="id")))

    def test_join_with_all_params(self):
        pdf1, pdf2 = self.data()
        df1 = self.spark.createDataFrame(pdf1)
        df2 = self.spark.createDataFrame(pdf2)
        actual = df1.asofJoin(df2, df1["time"], df2["time"], df1["id"], df2["id"],
            "1d", True).toPandas().sort_values(by='time').reset_index(drop=True)
        self.assertTrue(actual.equals(pd.merge_asof(pdf1, pdf2, on="time", by="id",
            tolerance=pd.Timedelta('1d'), allow_exact_matches=True)))

if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_asofjoin import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
