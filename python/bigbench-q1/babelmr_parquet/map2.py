from fs import local_read, local_write
import time
import pandas as pd
import itertools
from collections import Counter


def map2():
    TIMESTAMP = time.time()
    store_sales = local_read()
    IMPORT_TIME = time.time()
    sold_together = store_sales.groupby("ss_ticket_number")["item_id"].unique()

    counted_pairs = Counter()
    for items in sold_together:
        counted_pairs.update(itertools.combinations(sorted(items), 2))
    CALC_TIME = time.time()

    counted_pairs = (
        pd.DataFrame.from_dict(counted_pairs, orient="index")
        .reset_index()
        .rename(columns={"index": "item_pairs", 0: "count"})
    )

    counted_pairs[["item1", "item2"]] = pd.DataFrame(
        counted_pairs["item_pairs"].tolist(), index=counted_pairs.index
    )
    del counted_pairs["item_pairs"]


    local_write(
        counted_pairs,
    )

    EXPORT_TIME = time.time()
    return {
        "timestamp": TIMESTAMP * 1000,
        "Import": IMPORT_TIME * 1000,
        "Calculation": CALC_TIME * 1000,
        "Export": EXPORT_TIME * 1000,
    }
