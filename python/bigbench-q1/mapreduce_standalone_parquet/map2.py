from fs import read_partitioned_parallel
import time
import pandas as pd
from fastparquet import write
import itertools
from collections import Counter


def map2(event, myopen):
    TIMESTAMP = time.time()
    store_sales = read_partitioned_parallel(
        event["Import"], ["item_id", "ss_ticket_number"], myopen
    )
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

    out_partitions = event["Export"].setdefault("number_partitions", 10)
    counted_pairs["partition"] = counted_pairs["item1"] % out_partitions
    counted_pairs.sort_values("partition", inplace=True)
    row_group_offsets = counted_pairs["partition"].searchsorted(
        range(out_partitions), side="left"
    )

    write(
        f"{event['Export']['bucket']}/{event['Export']['key']}",
        counted_pairs,
        row_group_offsets=row_group_offsets,
        open_with=myopen,
        write_index=False
    )

    EXPORT_TIME = time.time()
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


if __name__ == "__main__":
    map2(
        {
            "Partitions": 10,
            "Stores": [1, 2, 3],
            "Categories": [1, 2, 3, 4],
            "Import": [
                {
                    # "bucket": "rsws2022",
                    # "key": "Paul/data/tpc_ds/store_sales.parquet", "partitions": []}",
                    "bucket": "",
                    "key": "/home/fabian/Desktop/HPI1/research-implementation-db/skyrise-udfs/python/bigbench-q1/mapreduce_standalone/udf/map1_out.parquet",
                    "partitions": [],
                }
            ],
            "Export": {
                "bucket": "",
                "key": "/home/fabian/Desktop/HPI1/research-implementation-db/skyrise-udfs/python/bigbench-q1/mapreduce_standalone/udf/map2_out.parquet",
            },
        },
        open,
    )
