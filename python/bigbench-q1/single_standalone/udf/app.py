import pandas as pd
from fastparquet import ParquetFile, write
from collections import Counter
import s3fs
import itertools
import time


def lambda_handler(event, context):
    TIMESTAMP = time.time()
    s3 = s3fs.S3FileSystem()
    items = ParquetFile(
        event["Items"]["bucket"] + "/" + event["Items"]["key"], open_with=s3.open
    ).to_pandas()
    store_sales = ParquetFile(
        event["Sales"]["bucket"] + "/" + event["Sales"]["key"], open_with=s3.open
    ).to_pandas()
    IMPORT_TIME = time.time()
    filtered_items = items[
        items["i_category_id"].isin(event.setdefault("Stores", [1, 2, 3]))
    ].filter(["i_item_sk"])

    filtered_sales = store_sales[
        store_sales["ss_store_sk"].isin(
            event.setdefault("Categories", [10, 20, 33, 40, 50])
        )
    ].filter(["ss_item_sk", "ss_ticket_number"])
    filtered_sales["item_id"] = filtered_sales["ss_item_sk"]

    joined = filtered_sales.set_index("ss_item_sk").join(
        filtered_items.set_index("i_item_sk"), how="inner"
    )
    sold_together = joined.groupby("ss_ticket_number")["item_id"].unique()
    counted_pairs = Counter()
    for items in sold_together:
        counted_pairs.update(itertools.combinations(sorted(items), 2))
    counted_pairs = (
        pd.DataFrame.from_dict(counted_pairs, orient="index")
        .reset_index()
        .rename(columns={"index": "item_pairs", 0: "count"})
    )
    counted_pairs[["item1", "item2"]] = pd.DataFrame(
        counted_pairs["item_pairs"].tolist(), index=counted_pairs.index
    )
    del counted_pairs["item_pairs"]
    counted_pairs = counted_pairs[counted_pairs["count"] > 50]
    top_pairs = counted_pairs.sort_values("count", ascending=False).iloc[:50]
    CALC_TIME = time.time()
    write(
        event["Export"]["bucket"] + "/" + event["Export"]["key"],
        top_pairs,
        open_with=s3.open,
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
    lambda_handler(
        {
            "Stores": [10, 20, 33, 40, 50],
            "Categories": [1, 2, 3],
            "Items": {
                "bucket": "benchmark-data-sets",
                "key": "tpcx-bb/standard/parquet/sf1/item/item_0.parquet",
                "partitions": [],
            },
            "Sales": {
                "bucket": "benchmark-data-sets",
                "key": "tpcx-bb/standard/parquet/sf1/store_sales/0.parquet",
                "partitions": [],
            },
            "Export": {
                "bucket": "rsws2022",
                "key": "final_result/abcdef/bb-q1.parquet",
                "number_partitions": 10,
            },
        },
        None,
    )
