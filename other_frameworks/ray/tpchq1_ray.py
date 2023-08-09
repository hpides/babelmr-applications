import ray
import argparse 
from ray.data.aggregate import Max, Mean, Sum, Count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Invoke PySpark Service on AWS Elastic MapReduce (EMR)")

    parser.add_argument(
        "--sf",  help="sf to be used [1,10,100,1000]", default=1
    )
    args = parser.parse_args()
    sf = args.sf
    input_files = []
    for i in range (5 * sf):
        input_files.append(f"s3://rsws2022/tpch-csv/sf1/lineitem/{i}.csv")


    ds = ray.data.read_csv(f"./data/")

    ds.filter(lambda x: x["l_shipdate"] <= "1998-09-02")
    
    ds = ds.add_column("pre_1", lambda ds: ds["l_extendedprice"] * (1 - ds["l_discount"]))
    ds = ds.add_column("pre_2", lambda ds: ds["pre_1"] * (1 + ds["l_tax"]))
    
    ds = ds.add_column("groupcol", lambda ds: ds["l_returnflag"] + ds["l_linestatus"])
    grouped = ds.groupby("groupcol")
    grouped_ds = grouped.aggregate(Sum("l_quantity"), Sum("l_extendedprice"), Sum("pre_1"), Sum("pre_2"), Mean("l_quantity"), Mean("l_extendedprice"), Mean("l_discount"), Count())
    grouped_ds.sort("groupcol")
    grouped_ds = grouped_ds.add_column("l_returnflag", lambda row: row["groupcol"].str[0])
    grouped_ds = grouped_ds.add_column("l_linestatus", lambda row: row["groupcol"].str[1])
    grouped_ds = grouped_ds.drop_columns(["groupcol"])

    grouped_ds.repartition(1).write_csv(f"s3://rsws2022/combined{sf}")


# import ray

# ray.init('auto')

# ds = ray.data.read_parquet("s3://benchmark-data-sets/tpc-h/standard/parquet/sf1/lineitem/0.parquet")
# print(ds.schema())