import pywren
from fastparquet import ParquetFile
from concurrent.futures import ThreadPoolExecutor
import s3fs

from copy import deepcopy
import pandas as pd

import sys
import os
sys.path.append(os.path.join(sys.path[0], "..", "..", "util"))
from generate_plans import generate_tpch_standalone_plan

def read_partitioned(file_desc, columns, opener, filter=False):
    df = pd.DataFrame()
    input_file = f"{file_desc['bucket']}/{file_desc['key']}"
    partitions = file_desc["partitions"]
    pf = ParquetFile(input_file, open_with=opener)
    for i, rg in enumerate(pf):  # read only said partitions:
        if partitions == [] or i in partitions:
            if filter:
                partition_data = rg.to_pandas(
                    columns=columns, filters=[("l_shipdate", "<=", "1998-09-02")]
                )
                df = pd.concat(
                    [df, partition_data[partition_data["l_shipdate"] <= "1998-09-02"]]
                )
            else:
                df = pd.concat([df, rg.to_pandas(columns=columns)])
    return df


def wrapper_read_partitioned(columns, opener, filter=False):
    def read_partitioned_wrapped(x):
        return read_partitioned(x, columns, opener, filter)

    return read_partitioned_wrapped


def read_partitioned_parallel(file_descriptions, columns, opener, filter=False):
    with ThreadPoolExecutor(max_workers=min(16, len(file_descriptions))) as executor:
        result_dfs = list(
            executor.map(
                wrapper_read_partitioned(columns, opener, filter), file_descriptions
            )
        )
    return pd.concat(result_dfs)


def q1(event):
    columns = [
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_shipdate",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
    ]
    s3 = s3fs.S3FileSystem()
    myopen = s3.open

    df = read_partitioned_parallel(event["Import"], columns, myopen, filter=True)
    df["pre_1"] = df["l_extendedprice"] * (1 - df["l_discount"])
    df["pre_2"] = df["pre_1"] * (1 + df["l_tax"])
    grouped = df.groupby(["l_returnflag", "l_linestatus"])
    map_tpc_h_q1 = pd.DataFrame(
        {
            "sum_qty": grouped["l_quantity"].sum(),
            "sum_base_price": grouped["l_extendedprice"].sum(),
            "sum_disc_price": grouped["pre_1"].sum(),
            "sum_charge": grouped["pre_2"].sum(),
            "avg_qty": grouped["l_quantity"].sum(),
            "avg_price": grouped["l_extendedprice"].sum(),
            "avg_disc": grouped["l_discount"].sum(),
            "count_order": grouped.size(),
        }
    ).reset_index()
    # TODO: handle cases where partition ends up being empty (e.g. add null element so that row group can be created)
    out_partitions = event["Export"]["number_partitions"]
    map_tpc_h_q1["partition"] = (
        map_tpc_h_q1["l_returnflag"].apply(ord) // 2
        + map_tpc_h_q1["l_linestatus"].apply(ord) // 3
    ) % out_partitions
    map_tpc_h_q1.sort_values("partition", inplace=True)
    row_group_offsets = map_tpc_h_q1["partition"].searchsorted(
        range(out_partitions), side="left"
    )
    return (map_tpc_h_q1, row_group_offsets)

#reduce the grouped results per file
def q1_reduce(dfs_rgs):

    df = pd.concat([df_rg[0] for df_rg in dfs_rgs])
    KEY_VAL = {}
    for i, row in enumerate(zip(df["l_returnflag"], df["l_linestatus"])):
        values_for_key = KEY_VAL.setdefault(row, [])
        values_for_key.append(df.iloc[i])

    return_df_list = []
    for key, values in KEY_VAL.items():
        agg = deepcopy(values[0])
        for val in values[1:]:
            for x in [
                "sum_qty",
                "sum_base_price",
                "sum_disc_price",
                "sum_charge",
                "avg_qty",
                "avg_price",
                "avg_disc",
                "count_order",
            ]:
                agg[x] += val[x]
        agg["avg_qty"] /= agg["count_order"]
        agg["avg_price"] /= agg["count_order"]
        agg["avg_disc"] /= agg["count_order"]
        return_df_list.append(agg)
    df = pd.DataFrame(return_df_list).sort_values(["l_returnflag", "l_linestatus"])
    return df

scale_factor = int(sys.argv[1])
if scale_factor not in [1 , 10, 100, 1000]:
    exit(1)
#get a pywren executors
plan = generate_tpch_standalone_plan(scale_factor)
keys_map = plan[0]["map"]
wrenexec = pywren.default_executor()
#run the query on pywren keys[0] is the Input folder so we ignore this one
futures = wrenexec.map(q1, keys_map)

#wait for futures to fill
raw=pywren.get_all_results(futures)

#reduce the results
futures_reduce = wrenexec.map(q1_reduce, [raw])

res_reduce=pywren.get_all_results(futures_reduce)
print(res_reduce)
