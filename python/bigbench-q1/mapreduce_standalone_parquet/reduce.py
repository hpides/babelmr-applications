from fs import read_partitioned_parallel
import time
from fastparquet import write


def reduce(event, myopen):
    TIMESTAMP = time.time()
    pairs = read_partitioned_parallel(
        event["Import"], ["item1", "item2", "count"], myopen
    )
    IMPORT_TIME = time.time()
    pairs = pairs.groupby(["item1", "item2"])
    reduced = (
        pairs["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
        .iloc[:50]
    )
    reduced = reduced[reduced["count"] > 50]
    CALC_TIME = time.time()

    write(
        f"{event['Export']['bucket']}/{event['Export']['key']}",
        reduced,
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
    reduce(
        {
            "Partitions": 10,
            "Stores": [1, 2, 3],
            "Categories": [1, 2, 3, 4],
            "Pairs": [
                {
                    # "bucket": "rsws2022",
                    # "key": "Paul/data/tpc_ds/store_sales.parquet", "partitions": []}",
                    "bucket": "",
                    "key": "skyrise-udfs/python/bigbench-q1/mapreduce_standalone/udf/map2_out.parquet",
                    "partitions": [],
                }
            ],
            "Export": {
                "bucket": "",
                "key": "skyrise-udfs/python/bigbench-q1/mapreduce_standalone/udf/reduce_out.parquet",
            },
        },
        open,
    )
