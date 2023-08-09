import sys
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import count, avg, sum


if __name__ == "__main__":
  sf = 1
  if len(sys.argv) > 1:
    sf = sys.argv[1]
          
  spark = SparkSession.builder.appName("tpc_h_q1").getOrCreate()
  df = spark.read.csv(f"s3://rsws2022/tpch-csv/sf{sf}/lineitem/", header=True, inferSchema=True)

  preprocessed = df.withColumn('pre_1', df["l_extendedprice"] * (1 - df["l_discount"])).withColumn("pre_2", df["l_extendedprice"] * (1 - df["l_discount"]) * (1 + df["l_tax"]))
  resultdf = preprocessed.filter(preprocessed["l_shipdate"] <= "1998-09-02").groupby("l_returnflag", "l_linestatus").agg(sum("l_quantity").alias("sum_qty"), sum("l_extendedprice").alias("sum_base_price"), sum("pre_1").alias("sum_disc_price"), sum("pre_2").alias("sum_charge"), avg("l_quantity").alias("avg_qty"),avg("l_extendedprice").alias("avg_price"), avg("l_discount").alias("avg_disc"), count("*").alias("count_order"))

  resultdf.coalesce(1).write.option("header", "true").mode("overwrite").csv("s3://rsws2022/code/pyspark/results/")