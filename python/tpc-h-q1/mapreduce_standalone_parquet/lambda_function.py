import pandas as pd
from fastparquet import ParquetFile, write
import s3fs
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import time
TIMESTAMP = 0
IMPORT_TIME = 0
CALC_TIME = 0
EXPORT_TIME = 0


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


def map(columns, s3, event, myopen):
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME
    df = read_partitioned_parallel(event["Import"], columns, myopen, filter=True)
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
    row_group_offsets = map_tpc_h_q1["partition"].searchsorted(
        range(out_partitions), side="left"
    )
    write(
        f"{event['Export']['bucket']}/{event['Export']['key']}",
        map_tpc_h_q1,
        row_group_offsets=row_group_offsets,
        open_with=myopen,
        write_index=False
    )
    EXPORT_TIME = time.time()


def reduce(columns, s3, event, myopen):
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME
    df = read_partitioned_parallel(event["Import"], columns, myopen)
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
    write(f"{event['Export']['bucket']}/{event['Export']['key']}", df, open_with=myopen, write_index=False)
    EXPORT_TIME = time.time()


def lambda_handler(event, context):
    TIMESTAMP = time.time()
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
    if event["mode"] == "map":
        map(columns, s3, event, myopen)
    elif event["mode"] == "reduce":
        reduce(None, s3, event, myopen)

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
