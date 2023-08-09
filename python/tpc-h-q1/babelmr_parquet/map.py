import pandas as pd
from copy import deepcopy
import time

from fs import local_read, local_write


def map():
    TIMESTAMP = time.time()
    df = local_read(filters=[("l_shipdate", "<=", "1998-09-02")])
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
    out_partitions = 4
    map_tpc_h_q1["partition"] = (
        map_tpc_h_q1["l_returnflag"].apply(ord) // 2
        + map_tpc_h_q1["l_linestatus"].apply(ord) // 3
    ) % out_partitions
    map_tpc_h_q1.sort_values("partition", inplace=True)
    row_group_offsets = map_tpc_h_q1["partition"].searchsorted(
        range(out_partitions), side="left"
    )
    local_write(map_tpc_h_q1, row_group_offsets)
    EXPORT_TIME = time.time()

    return {
        "timestamp": TIMESTAMP * 1000,
        "Import": IMPORT_TIME * 1000,
        "Calculation": CALC_TIME * 1000,
        "Export": EXPORT_TIME * 1000,
    }
