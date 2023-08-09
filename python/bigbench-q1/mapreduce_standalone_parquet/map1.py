from fs import read_partitioned_parallel
import time
from fastparquet import write


def map1(event, myopen):
    TIMESTAMP = time.time()
    store_sales = read_partitioned_parallel(
        event["Sales"],
        ["ss_ticket_number", "ss_store_sk", "ss_item_sk"],
        myopen,
    )
    items = read_partitioned_parallel(
        event["Items"],
        ["i_category_id", "i_item_sk"],
        myopen,
    )

    IMPORT_TIME = time.time()

    filtered_items = items[
        items["i_category_id"].isin(event.setdefault("Categories", [1, 2, 3]))
    ].filter(["i_item_sk"])

    filtered_sales = store_sales[
        store_sales["ss_store_sk"].isin(
            event.setdefault("Stores", [10, 20, 33, 40, 50])
        )
    ].filter(["ss_item_sk", "ss_ticket_number"])

    filtered_sales["item_id"] = filtered_sales["ss_item_sk"]
    joined = filtered_sales.set_index("ss_item_sk").join(
        filtered_items.set_index("i_item_sk"), how="inner"
    )
    out_partitions = event["Export"].setdefault("number_partitions", 10)
    joined["partition"] = joined["ss_ticket_number"] % out_partitions
    joined.sort_values("partition", inplace=True)
    row_group_offsets = joined["partition"].searchsorted(
        range(out_partitions), side="left"
    )
    CALC_TIME = time.time()
    write(
        f"{event['Export']['bucket']}/{event['Export']['key']}",
        joined,
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
    map1(
        {
            "Partitions": 10,
            "Stores": [10, 20, 33, 40, 50],
            "Categories": [1, 2, 3],
            "Items": [
                {
                    #                    "bucket": "rsws2022",
                    #                    "key": "Paul/data/tpc_ds/test/item.parquet",
                    "bucket": "",
                    "key": "skyrise-udfs/python/bigbench-q1/mapreduce_standalone/udf/item.parquet",
                    "partitions": [],
                }
            ],
            "Sales": [
                {
                    # "bucket": "rsws2022",
                    # "key": "Paul/data/tpc_ds/store_sales.parquet",
                    "bucket": "",
                    "key": "skyrise-udfs/python/bigbench-q1/mapreduce_standalone/udf/store_sales.parquet",
                    "partitions": [],
                }
            ],
            "Export": {
                "bucket": "",
                "key": "skyrise-udfs/python/bigbench-q1/mapreduce_standalone/udf/map1_out.parquet",
            },
        },
        open,
    )
