from common.variables import *
from common.seo import *
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType
from common.spark_udfs import *

import sys

conf = SparkConf()
sc = SparkContext(conf=conf)


def main():
    sqlContext = SQLContext(sc)
    spark_app_conf = sc.getConf()
    # -------------------------------------------------------------------------------------------
    # 1. Check if Spark is local
    # -------------------------------------------------------------------------------------------
    spark_is_local = 'local' in spark_app_conf.get("spark.master")

    # -------------------------------------------------------------------------------------------
    # 2. Configure parameters accordingly
    # -------------------------------------------------------------------------------------------
    ip_file = args.ip_file
    if spark_is_local:
        # Amend file path
        ip_file = "file://" + args.ip_file

    # -------------------------------------------------------------------------------------------
    # 3. Read input file
    # -------------------------------------------------------------------------------------------
    df = sqlContext.read.json(ip_file)

    # -------------------------------------------------------------------------------------------
    # 4. Filter 'Chilies'
    # -------------------------------------------------------------------------------------------

    # Please refer to function definition where more information could be found regarding this approach.
    candidates = getCandidates(args.key_ing.lower(), int(args.edit_dist), sc)

    filterKeywords = getUDF_filterKeywords(candidates, udf, BooleanType)

    df_filtered = df.filter(filterKeywords(df.ingredients))

    # -------------------------------------------------------------------------------------------
    # 5. Determine difficulty
    # -------------------------------------------------------------------------------------------
    get_diff = getUDF_procDiff(udf, StringType)
    df_diff = df_filtered.withColumn("difficulty", get_diff(df_filtered.cookTime, df_filtered.prepTime))

    # -------------------------------------------------------------------------------------------
    # 6. Save as parquet
    # -------------------------------------------------------------------------------------------
    df_diff.write.mode("Overwrite").parquet(args.op_path)
    sys.exit(0)


if __name__ == "__main__":
    main()
