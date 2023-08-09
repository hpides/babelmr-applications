import pandas as pd
from copy import deepcopy
import time

from fs import local_read, local_write


def reduce():
    TIMESTAMP = time.time()
    df = local_read()
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
    local_write(df)
    EXPORT_TIME = time.time()

    return {
        "timestamp": TIMESTAMP * 1000,
        "Import": IMPORT_TIME * 1000,
        "Calculation": CALC_TIME * 1000,
        "Export": EXPORT_TIME * 1000,
    }
