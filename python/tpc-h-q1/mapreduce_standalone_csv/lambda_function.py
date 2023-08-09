import pandas as pd
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import time

TIMESTAMP = 0
IMPORT_TIME = 0
CALC_TIME = 0
EXPORT_TIME = 0

def read_partitioned(file_desc):
    input_file = f"{file_desc['bucket']}/{file_desc['key']}"
    df = pd.read_csv(f"s3://{input_file}")
    return df

def read_partitioned_parallel(file_descriptions):
    with ThreadPoolExecutor(max_workers=min(16, len(file_descriptions))) as executor:
        result_dfs = list(
            executor.map(read_partitioned, file_descriptions)
        )
    return pd.concat(result_dfs)

def map(event):
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME
    df = read_partitioned_parallel(event["Import"])
    df = df[df["l_shipdate"] <= "1998-09-02"]
    IMPORT_TIME = time.time()
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
    CALC_TIME = time.time()
    # TODO: handle cases where partition ends up being empty (e.g. add null element so that row group can be created)
    out_partitions = event["Export"]["number_partitions"]
    map_tpc_h_q1["partition"] = (
        map_tpc_h_q1["l_returnflag"].apply(ord) // 2
        + map_tpc_h_q1["l_linestatus"].apply(ord) // 3
    ) % out_partitions
    map_tpc_h_q1.sort_values("partition", inplace=True)
    map_tpc_h_q1.to_csv(f"s3://{event['Export']['bucket']}/{event['Export']['key']}")
    EXPORT_TIME = time.time()


def reduce(event):
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME
    df = read_partitioned_parallel(event["Import"])
    IMPORT_TIME = time.time()
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
    CALC_TIME = time.time()
    df.to_csv(f"s3://{event['Export']['bucket']}/{event['Export']['key']}")
    EXPORT_TIME = time.time()


def lambda_handler(event, context):
    TIMESTAMP = time.time()
    if event["mode"] == "map":
        map(event)
    elif event["mode"] == "reduce":
        reduce(event)

    return {
        "message": "success",
        "times": [
            {
                "timestamp": TIMESTAMP * 1000,
                "Import": IMPORT_TIME * 1000,
                "Calculation": CALC_TIME * 1000,
                "Export": EXPORT_TIME * 1000,
            }
        ],
    }
