#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Simple test example"""

from sparktestingbase.testcase import SparkTestingBaseTestCase
import unittest2

from common.seo import getCandidates
from common.spark_udfs import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType, StructType, StructField


class SimpleTest(SparkTestingBaseTestCase):
    """A simple test."""

    def test_filterKeywords(self):
        """Test filterKeywords."""
        sqlContext = SQLContext(self.sc)
        candidates = getCandidates("Chilies".lower(), 2, self.sc)

        filterKeywords = getUDF_filterKeywords(candidates, udf, BooleanType)
        input = [("no edit chilies",), ("edit 1 chilie",), ("edit 2 chili",), ("edit 3 chil",)]
        rdd = self.sc.parallelize(input)

        fields = [StructField("ingredients", StringType(), True)]
        schema = StructType(fields)
        df = sqlContext.createDataFrame(rdd, schema)

        df_filtered = df.filter(filterKeywords(df.ingredients))
        assert df_filtered.count() == 3

    def test_procDiff(self):
        """Test process difficulty."""
        sqlContext = SQLContext(self.sc)

        input = [("PT25M", "PT10M"), ("PT25M", "PT2M"), ("PT30M", ""), ("PT100M", "PT1M"), ("PT1H", "PT1M"),
                 ("PT1H3M", "PT2M"), ("", "PT1M"), ("", "")]
        rdd = self.sc.parallelize(input)
        fields = [StructField("cookTime", StringType(), True), StructField("prepTime", StringType(), True)]
        schema = StructType(fields)
        df = sqlContext.createDataFrame(rdd, schema)

        get_diff = getUDF_procDiff(udf, StringType)
        result = df.withColumn("difficulty", get_diff(df.cookTime, df.prepTime)).select("difficulty").collect()
        assert map(lambda res: res['difficulty'], result) == ["Medium", "Easy", "Unknown", "Hard", "Hard", "Hard",
                                                              "Unknown", "Unknown"]

if __name__ == "__main__":
    unittest2.main()
